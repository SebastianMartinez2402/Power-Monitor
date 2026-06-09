/* ============================================================
   Smart Energy Contact — app.js
   MQTT via Paho (WebSockets) · Mosquitto puerto 9001
   ============================================================ */

// ------------------------------------------------------------------
// Estado global
// ------------------------------------------------------------------
let client    = null;
let connected = false;
let relayOn   = null; // Estado real desconocido hasta recibir confirmación del hardware
let powerChartRange = 'hour';
let consumptionChartRange = 'week';

let powerChart = null;
const powerChartLabels = [];
const powerChartData = [];

let consumptionChart = null;
const consumptionChartLabels = [];
const consumptionChartData = [];

let pendingConsumptionKwh = 0;
let lastConsumptionSaveTime = Date.now();

let latestAlertTimeout = null;
let lastDisplayedFaultFlags = 0;

// Registro local de alertas en memoria (respaldo si Supabase no está disponible)
// Cada entrada: { time: string, label: string, severity: string }
const localAlertLog = [];

let harmonicChart = null;
const harmonicChartLabels = [];
const harmonicChartData = [];
let waveformChart = null;

const HARMONICS_TOPIC = 'smartcontact/contacto_01/telemetria/armonicos';
const HARMONICS_REQUEST_TOPIC = 'smartcontact/contacto_01/control/armonicos/request';
const HARMONICS_COUNT = 20;

const WAVEFORM_REQUEST_TOPIC = 'smartcontact/contacto_01/control/waveform/request';
const WAVEFORM_RESPONSE_TOPIC = 'smartcontact/contacto_01/telemetria/waveform';

// Formato binario de forma de onda
const WAVEFORM_HEADER_SIZE = 24;
const WAVEFORM_CHANNELS_EXPECTED = 2;
const WAVEFORM_FORMAT_I16_SCALED = 1;

const VOLTAGE_SCALE = 100.0;
const CURRENT_SCALE = 1000.0;

const waveformSequences = new Map();

const SUPABASE_URL = 'https://ukoqchpjwvbilbakwumn.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVrb3FjaHBqd3ZiaWxiYWt3dW1uIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODA1MjU3MjYsImV4cCI6MjA5NjEwMTcyNn0.-mu33cjuG24s9ONlSjCzhVGDJnOD3jMTify56yBJn-c';

const db = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function testDatabaseConnection() {
  try {
    const { data, error } = await db
      .from('telemetry')
      .select('id, created_at')
      .limit(1);

    if (error) {
      log(`Error conectando a Supabase: ${error.message}`, 'error');
      return;
    }

    log('Conexión a Supabase correcta.', 'success');
  } catch (e) {
    log(`Error Supabase: ${e.message}`, 'error');
  }
}


// Acumulador de energía (kWh)
let kwhTotal      = 0;       // kWh acumulados en sesión
let kwhStartTime  = null;    // timestamp de inicio de sesión
let lastPowerW    = 0;       // última potencia activa recibida (W)
let kwhTimerInterval = null;
let lastFaultFlags = 0;

let dataWatchdog = null;

let periodStartTime = null;
let periodStartKwh = 0;

// Límites para las barras de progreso
const LIMITS = { v: 250, i: 10 };

// ------------------------------------------------------------------
// Registro dinámico de dispositivos detectados
// Se puebla automáticamente al recibir telemetría de cualquier ESP32.
// El dashboard extrae el device ID del tópico y lo guarda aquí.
// Así publishToAllDevices() sabe a cuántos y cuáles enviar.
// ------------------------------------------------------------------
const knownDevices = new Set(['contacto_01']); // seed: el dispositivo hardcodeado

function registerDeviceFromTopic(topic) {
  // Los tópicos siguen el patrón: smartcontact/<device_id>/...
  const parts = topic.split('/');
  if (parts.length >= 2 && parts[0] === 'smartcontact') {
    const deviceId = parts[1];
    if (deviceId && !knownDevices.has(deviceId)) {
      knownDevices.add(deviceId);
      log(`✔ Nuevo dispositivo detectado: ${deviceId} (total: ${knownDevices.size})`, 'success');
    }
  }
}

// Publica un mensaje en el tópico de control de CADA dispositivo conocido
function publishToAllDevices(controlPath, payload) {
  if (!connected || !client) {
    log('Error: No conectado. No se puede enviar comando.', 'error');
    return;
  }
  knownDevices.forEach(deviceId => {
    const topic = `smartcontact/${deviceId}/${controlPath}`;
    const message = new Paho.MQTT.Message(payload.toString());
    message.destinationName = topic;
    client.send(message);
  });
  log(`▸ Enviado a ${knownDevices.size} dispositivo(s) [${controlPath}]: ${payload}`, 'info');
}

const FAULTS = {
  FAULT_OVERCURRENT: {
    bit: 0,
    label: 'Sobrecorriente',
    severity: 'error'
  },
  FAULT_OVERVOLTAGE: {
    bit: 1,
    label: 'Sobrevoltaje',
    severity: 'error'
  },
  FAULT_UNDERVOLTAGE: {
    bit: 2,
    label: 'Bajo voltaje',
    severity: 'warn'
  },
  FAULT_OVERPOWER: {
    bit: 3,
    label: 'Sobrecarga de potencia',
    severity: 'error'
  },
  FAULT_FREQUENCY_OUT_OF_RANGE: {
    bit: 4,
    label: 'Frecuencia fuera de rango',
    severity: 'warn'
  },
  FAULT_HIGH_THD: {
    bit: 5,
    label: 'THD elevado',
    severity: 'warn'
  },
  FAULT_POWER_FACTOR_TOO_LOW: {
    bit: 6,
    label: 'Factor de potencia bajo',
    severity: 'warn'
  },
  FAULT_NO_VOLTAGE: {
    bit: 7,
    label: 'Sin voltaje detectado',
    severity: 'error'
  },
  FAULT_NO_LOAD: {
    bit: 8,
    label: 'Sin corriente cuando se esperaba carga',
    severity: 'warn'
  },
  FAULT_CURRENT_WHEN_RELAY_OPEN: {
    bit: 9,
    label: 'Corriente detectada con relé abierto',
    severity: 'error'
  },
  FAULT_ADC_SATURATION: {
    bit: 10,
    label: 'Saturación del ADC',
    severity: 'error'
  },
  FAULT_ADC_DISCONNECTED: {
    bit: 11,
    label: 'ADC desconectado',
    severity: 'error'
  },
  FAULT_ZERO_CROSS_MISSING: {
    bit: 12,
    label: 'Cruce por cero ausente',
    severity: 'error'
  },
  FAULT_ZERO_CROSS_STUCK: {
    bit: 13,
    label: 'Cruce por cero bloqueado',
    severity: 'error'
  },
  FAULT_RELAY_WELDED: {
    bit: 14,
    label: 'Relé soldado',
    severity: 'error'
  },
  FAULT_RELAY_FAILED_TO_CLOSE: {
    bit: 15,
    label: 'Relé no cerró',
    severity: 'error'
  },
  FAULT_RELAY_FAILED_TO_OPEN: {
    bit: 16,
    label: 'Relé no abrió',
    severity: 'error'
  },
  FAULT_HIGH_POWER: {
    bit: 17,
    label: 'Potencia elevada',
    severity: 'warn'
  }
};

const CFE_TARIFF_1C = {
  name: 'CFE 1C MTY/NL - Junio 2026',
  currency: 'MXN',
  blocks: [
    {
      label: 'Básico',
      limitKwh: 150,
      price: 1.1250
    },
    {
      label: 'Intermedio',
      limitKwh: 150,
      price: 1.3690
    },
    {
      label: 'Excedente',
      limitKwh: Infinity,
      price: 4.0040
    }
  ],
  dacLimitKwhPerMonth: 850
};

// ------------------------------------------------------------------
function getActiveFaults(faultFlags) {
  const flags = Number(faultFlags);

  if (!Number.isFinite(flags) || flags === 0) {
    return [];
  }

  const activeFaults = [];

  Object.entries(FAULTS).forEach(([code, info]) => {
    const mask = 1 << info.bit;

    if ((flags & mask) !== 0) {
      activeFaults.push({
        code,
        label: info.label,
        severity: info.severity
      });
    }
  });

  return activeFaults;
}

async function saveTelemetryToDatabase(d) {
  try {
    const { error } = await db
      .from('telemetry')
      .insert({
        voltage: d.v ?? null,
        current: d.i ?? null,
        active_power: d.p_activa ?? null,
        apparent_power: d.p_aparente ?? null,
        reactive_power: d.p_reactiva ?? null,
        power_factor: d.fp ?? null,
        thd: d.thd ?? null,
        fault_flags: d.fault_flags ?? null
      });

    if (error) {
      log(`Error guardando telemetría DB: ${error.message}`, 'error');
      return;
    }

    log('Telemetría guardada en Supabase.', 'success');
  } catch (e) {
    log(`Error DB telemetría: ${e.message}`, 'error');
  }
}

function showLatestAlert(faultFlags) {
  const alertBox = $('latestAlert');
  const alertText = $('latestAlertText');

  if (!alertBox || !alertText) return;

  const activeFaults = getActiveFaults(faultFlags);

  if (activeFaults.length === 0) {
    scheduleHideLatestAlert();
    return;
  }

  // Mientras la ESP32 siga mandando alerta, se cancela el ocultamiento
  if (latestAlertTimeout) {
    clearTimeout(latestAlertTimeout);
    latestAlertTimeout = null;
  }

  // Priorizar errores sobre warnings
  const selectedFault =
    activeFaults.find(fault => fault.severity === 'error') || activeFaults[0];

  const extraCount = activeFaults.length - 1;
  const extraText = extraCount > 0 ? ` +${extraCount}` : '';

  alertText.textContent = `${selectedFault.label}${extraText}`;

  alertBox.classList.remove('hidden', 'warn', 'error');
  alertBox.classList.add(selectedFault.severity === 'error' ? 'error' : 'warn');

  lastDisplayedFaultFlags = Number(faultFlags);
}

function scheduleHideLatestAlert() {
  const alertBox = $('latestAlert');
  const alertText = $('latestAlertText');

  if (!alertBox || !alertText) return;

  if (latestAlertTimeout) {
    clearTimeout(latestAlertTimeout);
  }

  latestAlertTimeout = setTimeout(() => {
    alertBox.classList.add('hidden');
    alertText.textContent = '—';
    lastDisplayedFaultFlags = 0;
  }, 5000);
}

// ------------------------------------------------------------------
// Modal de alertas
// ------------------------------------------------------------------
let alertModalQueue = [];
let alertModalVisible = false;

function showAlertModal(faults) {
  // faults: array de { label, severity }
  if (!faults || faults.length === 0) return;

  // Encolar todas las fallas nuevas
  faults.forEach(f => alertModalQueue.push(f));

  if (!alertModalVisible) {
    _renderNextAlertModal();
  }
}

function _renderNextAlertModal() {
  if (alertModalQueue.length === 0) {
    alertModalVisible = false;
    return;
  }

  alertModalVisible = true;
  const fault = alertModalQueue.shift();

  // Crear overlay
  const overlay = document.createElement('div');
  overlay.id = 'alertModalOverlay';
  overlay.style.cssText = `
    position: fixed; inset: 0; z-index: 9999;
    background: rgba(0,0,0,0.65); backdrop-filter: blur(3px);
    display: flex; align-items: center; justify-content: center;
    animation: fadeInOverlay .15s ease;
  `;

  const isError = fault.severity === 'error';
  const accentColor = isError ? '#ff4757' : '#ffcc00';
  const iconChar = isError ? '🚨' : '⚠️';

  const box = document.createElement('div');
  box.style.cssText = `
    background: #0d1825;
    border: 2px solid ${accentColor};
    border-radius: 10px;
    padding: 28px 32px 24px;
    min-width: 320px; max-width: 480px;
    box-shadow: 0 0 32px ${accentColor}55;
    font-family: 'Exo 2', sans-serif;
    color: #c8d8f0;
    text-align: center;
    animation: slideInModal .2s ease;
  `;

  const remaining = alertModalQueue.length;
  const moreText = remaining > 0 ? `<div style="margin-top:8px;font-size:12px;color:#4a6080">+${remaining} alerta${remaining > 1 ? 's' : ''} pendiente${remaining > 1 ? 's' : ''}</div>` : '';

  box.innerHTML = `
    <div style="font-size:38px;margin-bottom:12px">${iconChar}</div>
    <div style="font-size:13px;letter-spacing:2px;text-transform:uppercase;color:${accentColor};margin-bottom:8px">
      ${isError ? 'ALERTA CRÍTICA' : 'ADVERTENCIA'}
    </div>
    <div style="font-size:18px;font-weight:600;margin-bottom:20px">${fault.label}</div>
    <div style="font-size:11px;color:#4a6080;margin-bottom:20px">${new Date().toLocaleString('es-MX')}</div>
    ${moreText}
    <button id="alertModalCloseBtn" style="
      margin-top:18px;
      background:${accentColor}22; border:1px solid ${accentColor};
      color:${accentColor}; border-radius:6px; padding:8px 28px;
      font-size:13px; letter-spacing:1px; cursor:pointer;
      font-family:inherit; text-transform:uppercase;
    ">Aceptar</button>
  `;

  overlay.appendChild(box);
  document.body.appendChild(overlay);

  const closeModal = () => {
    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
    // Si hay más en cola, mostrar la siguiente con pequeño delay
    if (alertModalQueue.length > 0) {
      setTimeout(_renderNextAlertModal, 300);
    } else {
      alertModalVisible = false;
    }
  };

  document.getElementById('alertModalCloseBtn').addEventListener('click', closeModal);
  // Todos los modales (error y warn) esperan click del usuario — sin auto-cierre
}

// Inyectar keyframes para la animación del modal (una sola vez)
(function injectModalStyles() {
  if (document.getElementById('alertModalStyles')) return;
  const style = document.createElement('style');
  style.id = 'alertModalStyles';
  style.textContent = `
    @keyframes fadeInOverlay { from { opacity:0 } to { opacity:1 } }
    @keyframes slideInModal  { from { transform:translateY(-20px); opacity:0 } to { transform:translateY(0); opacity:1 } }
  `;
  document.head.appendChild(style);
})();

// ------------------------------------------------------------------
// Referencias DOM
// ------------------------------------------------------------------
const $ = id => document.getElementById(id);

const els = {
  v:   $('val-v'),
  i:   $('val-i'),
  pa:  $('val-pa'),
  pap: $('val-pap'),
  pr:  $('val-pr'),
  fp:  $('val-fp'),
  thd: $('val-thd'),
  kwh: $('val-kwh'),
};

// ------------------------------------------------------------------
// Conectar / Desconectar
// ------------------------------------------------------------------
window.toggleConnection = function () {
  connected ? disconnect() : connect();
};

function startOfToday() {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function endOfNow() {
  return Date.now();
}

function getStartTimeForRange(range) {
  const todayStart = startOfToday();

  if (range === 'hour') {
    return Date.now() - (60 * 60 * 1000);
  }

  if (range === 'day') {
    return todayStart;
  }

  if (range === 'week') {
    // Hoy + 6 días anteriores = 7 días exactos
    return todayStart - (6 * 24 * 60 * 60 * 1000);
  }

  if (range === 'bimester') {
    // 9 semanas exactas, no 10 buckets parciales
    return todayStart - (8 * 7 * 24 * 60 * 60 * 1000);
  }

  return Date.now() - (60 * 60 * 1000);
}

function handleDataTimeout() {
  log('⚠ Alerta: Se dejó de recibir telemetría. Reiniciando valores a 0.', 'warn');

  // 1. Mandar un objeto con puros ceros para actualizar las tarjetas y la gráfica
  updateDashboard({ 
    v: 0, i: 0, p_activa: 0, p_aparente: 0, p_reactiva: 0, fp: 0, thd: 0 
  });

  // Nota: Al mandar p_activa: 0, lastPowerW se vuelve 0, 
  // lo que hace que el contador de kWh se congele automáticamente.

  // 2. Apagar el relé visualmente por seguridad
  if (relayOn) {
    relayOn = false;
    saveConsumptionPeriod(); // Guardar el historial de consumo hasta este corte

    const btn  = $('onoffBtn');
    const text = $('onoffText');
    const hint = $('onoffHint');
    
    if (btn && text && hint) {
      btn.className  = 'onoff-btn onoff-off';
      text.textContent = 'OFF';
      hint.textContent = 'Sin conexión de datos';
    }
  }
}

function connect() {
  const host  = $('brokerHost').value.trim() || 'localhost';
  const port  = parseInt($('brokerPort').value) || 9001;
  const topic = 'sec/datos';

  const clientId = 'sec_dashboard_' + Math.random().toString(16).slice(2, 8);
  log(`Conectando a ws://${host}:${port} …`, 'info');

  client = new Paho.MQTT.Client(host, port, clientId);
  client.onConnectionLost = onConnectionLost;
  // Paho 1.0.1 falla con mensajes binarios si se accede a payloadString.
  // Parcheamos onMessageArrived para interceptar el tópico waveform
  // y leer el ArrayBuffer interno ANTES de que Paho intente decodificar UTF-8.
  client.onMessageArrived = function(message) {
    try {
      if (message.destinationName.match(/^smartcontact\/.+\/telemetria\/waveform$/)) {
        _handleWaveformMessage(message);
        return;
      }
    } catch (e) {
      log(`Error interceptando waveform: ${e.message}`, 'error');
      return;
    }
    onMessageArrived(message);
  };

  client.connect({
    onSuccess:  () => onConnected(topic),
    onFailure:  (err) => onConnectFailed(err),
    useSSL:     false,
    timeout:    8,
    keepAliveInterval: 30,
  });
}

function disconnect() {
  if (dataWatchdog) { clearTimeout(dataWatchdog); dataWatchdog = null; }
  if (client && connected) client.disconnect();
  setStatus(false);
  log('Desconectado manualmente.', 'warn');
}

// ------------------------------------------------------------------
// Callbacks MQTT
// ------------------------------------------------------------------
function onConnected(topic) {
  connected = true;
  setStatus(true);
  log(`Conectado. Suscrito a "${topic}"`, 'success');

  // 1. Suscripción a la telemetría (la que el usuario pone en la interfaz)
  client.subscribe(topic, {
    onSuccess:  () => log(`✔ Suscripción a "${topic}" confirmada.`, 'success'),
    onFailure:  (err) => log(`Error suscripción: ${err.errorMessage}`, 'error'),    
  });

  client.subscribe('smartcontact/+/telemetria/estado', {
    onSuccess: () => log('✔ Suscripción a telemetría/estado (todos los dispositivos) confirmada.', 'success'),
    onFailure: err => log(`Error suscripción telemetría/estado: ${err.errorMessage}`, 'error'),
  });  

  // Suscripción a las alertas de todos los dispositivos
  client.subscribe('smartcontact/+/alertas');

  // Suscripción al estado físico del relé de todos los dispositivos
  client.subscribe('smartcontact/+/estado/rele');
  client.subscribe('smartcontact/+/telemetria/armonicos', {
    onSuccess: () => log(`✔ Suscripción a armónicos (todos los dispositivos) confirmada.`, 'success'),
    onFailure: err => log(`Error suscripción armónicos: ${err.errorMessage}`, 'error'),
  });

  client.subscribe('smartcontact/+/telemetria/waveform', {
    onSuccess: () => log(`✔ Suscripción a waveform (todos los dispositivos) confirmada.`, 'success'),
    onFailure: err => log(`Error suscripción waveform: ${err.errorMessage}`, 'error'),
  });

  const btn = $('connectBtn');
  btn.textContent = 'Desconectar';
  btn.classList.add('disconnect');

  // Iniciar acumulador kWh
  if (!kwhStartTime) startKwhTimer();
}

function onConnectionLost(res) {
  connected = false;
  setStatus(false);
  if (res.errorCode !== 0) log(`Conexión perdida: ${res.errorMessage}`, 'error');
  const btn = $('connectBtn');
  btn.textContent = 'Conectar';
  btn.classList.remove('disconnect');
}

function onConnectFailed(err) {
  log(`No se pudo conectar: ${err.errorMessage}`, 'error');
}

function onMessageArrived(message) {
  const topic = message.destinationName;
  const raw = message.payloadString;
  const telemetriaTopic = 'sec/datos';

  // Registrar automáticamente cualquier dispositivo que publique bajo smartcontact/
  if (topic.startsWith('smartcontact/')) {
    registerDeviceFromTopic(topic);
  }

  // ============================================================
  // FLUJO A: Telemetría normal (Datos para tus gráficas)
  // ============================================================
  if (topic === telemetriaTopic || topic.match(/^smartcontact\/.+\/telemetria\/estado$/)) {
    log(`← ${raw}`, 'data');
    try {
      const d = JSON.parse(raw);
      updateDashboard(d);
      saveTelemetryToDatabase(d);
      $('lastUpdate').textContent = new Date().toLocaleTimeString('es-MX');

      // --- Watchdog Dinámico ---
      if (dataWatchdog) clearTimeout(dataWatchdog);
      
      // 1. Leer el tiempo de muestreo actual de la interfaz (en segundos)
      const tiempoMuestreoSegundos = parseInt($('sampleRate').value) || 1;
      
      // 2. Convertir a milisegundos y sumar el margen de tolerancia (2.5s)
      const toleranciaMs = 2500; 
      const timeoutDinamicoMs = (tiempoMuestreoSegundos * 1000) + toleranciaMs;
      
      // 3. Iniciar el temporizador
      dataWatchdog = setTimeout(handleDataTimeout, timeoutDinamicoMs);
      // ---------------------------------------------------

    } catch (e) {
      log(`JSON inválido: ${e.message}`, 'error');
    }
  }
  
  // ============================================================
  // FLUJO B: Alertas del ESP32 (Errores físicos detectados)
  // ============================================================
  else if (topic.match(/^smartcontact\/.+\/alertas$/)) {
    try {
      const payload = JSON.parse(raw);

      // El ESP32 manda: {timestamp, flags, active, severity, cleared}
      // timestamp: segundos Unix — lo convertimos a ms para Date()
      // active: bitmask de fallas activas en este momento
      // cleared: true si las fallas fueron resueltas

      const activeMask  = Number(payload.active  ?? payload.flags ?? 0);
      const timestampMs = Number(payload.timestamp ?? 0) * 1000;
      const cleared     = payload.cleared === true;

      // Construir fecha legible desde el timestamp del hardware
      // Si el timestamp es 0, inválido, o antes del año 2020, usar la fecha actual del navegador
      const MIN_VALID_TS_MS = new Date('2020-01-01').getTime();
      const fechaHardware = (timestampMs > MIN_VALID_TS_MS)
        ? new Date(timestampMs).toLocaleString('es-MX')
        : new Date().toLocaleString('es-MX');

      if (cleared || activeMask === 0) {
        log(`✔ Alertas resueltas (${fechaHardware})`, 'success');
        scheduleHideLatestAlert();
        return;
      }

      // Descomponer el bitmask en fallas individuales usando FAULTS
      const fallasActivas = Object.entries(FAULTS).filter(([, info]) => {
        return (activeMask & (1 << info.bit)) !== 0;
      });

      fallasActivas.forEach(([code, info]) => {
        // Mostrar en el log con nombre legible, no código
        log(`🚨 ALERTA [${fechaHardware}]: ${info.label}`, info.severity === 'error' ? 'error' : 'warn');

        // Corte de seguridad visual para fallas críticas
        if (info.severity === 'error' && (
          code === 'FAULT_OVERCURRENT' ||
          code === 'FAULT_OVERPOWER'   ||
          code === 'FAULT_RELAY_WELDED'
        )) {
          relayOn = false;
          const btn  = $('onoffBtn');
          const text = $('onoffText');
          const hint = $('onoffHint');
          if (btn && text && hint) {
            btn.className    = 'onoff-btn onoff-off';
            text.textContent = 'OFF';
            hint.textContent = 'Corte por seguridad';
          }
        }

        // Guardar en Supabase con timestamp real del hardware
        saveAlert({
          code,
          label:     info.label,
          severity:  info.severity,
          value:     activeMask,
          timestamp: fechaHardware
        });
      });

      // Mostrar la alerta más prioritaria junto al botón de conectar
      showLatestAlert(activeMask);

      // Abrir modal de notificación para cada falla activa
      // fallasActivas es [[code, info], ...] — mapear a { label, severity }
      showAlertModal(fallasActivas.map(([, info]) => ({ label: info.label, severity: info.severity })));

    } catch (e) {
      log(`Error procesando alerta: ${e.message}`, 'error');
    }
  } 
  
  // ============================================================
  // FLUJO C: Sincronización del botón físico del ESP32
  // ============================================================
  else if (topic.match(/^smartcontact\/.+\/estado\/rele$/)) {
    const estadoFisico = raw.trim().toUpperCase();
    
    const btn  = $('onoffBtn');
    const text = $('onoffText');
    const hint = $('onoffHint');

    if (estadoFisico === 'ON') {
      // Solo si estaba apagado iniciamos el cálculo de consumo
      if (!relayOn) {
        relayOn = true;
        periodStartTime = new Date().toLocaleString('es-MX');
        periodStartKwh = kwhTotal;
      }
      log('Sincronización: Relé encendido físicamente.', 'success');
      
      if (btn && text && hint) {
        btn.className  = 'onoff-btn onoff-on';
        text.textContent = 'ON';
        hint.textContent = 'Contacto energizado';
      }
    } else if (estadoFisico === 'OFF') {
      // Solo si estaba prendido cerramos el cálculo de consumo
      if (relayOn) {
        relayOn = false;
        saveConsumptionPeriod();
      }
      log('Sincronización: Relé apagado físicamente.', 'warn');
      
      if (btn && text && hint) {
        btn.className  = 'onoff-btn onoff-off';
        text.textContent = 'OFF';
        hint.textContent = 'Contacto apagado';
      }
    }
  }

  // ============================================================
  // FLUJO D: Armónicos THD en vivo
  // ============================================================
  else if (topic.match(/^smartcontact\/.+\/telemetria\/armonicos$/)) {
    log(`← Armónicos THD: ${raw}`, 'data');

    try {
      const payload = JSON.parse(raw);
      processHarmonicsPayload(payload);
    } catch (e) {
      log(`Error procesando armónicos: ${e.message}`, 'error');
    }
  }

  // FLUJO E: Forma de onda — manejado por _handleWaveformMessage antes de llegar aquí
  // (el dispatch ocurre en client.onMessageArrived para evitar que Paho procese
  //  el payload binario como UTF-8 y cierre la conexión)
  else if (topic.match(/^smartcontact\/.+\/telemetria\/waveform$/)) {
    // No debería llegar aquí; _handleWaveformMessage lo intercepta antes.
    log('Waveform llegó a onMessageArrived (inesperado).', 'warn');
  }
}

// ------------------------------------------------------------------
// Manejo seguro de mensajes binarios de forma de onda
// Paho 1.0.1 no soporta payloadBytes. Al acceder a payloadString en un
// mensaje binario puede lanzar una excepción interna que cierra la conexión.
// Esta función lee el buffer interno de Paho (_buffer o similar) directamente.
// ------------------------------------------------------------------
function _handleWaveformMessage(message) {
  let arrayBuffer = null;

  try {
    // Paho 1.0.1 almacena el payload como Uint8Array en message._buffer
    // o en message.payloadBytes dependiendo de la versión exacta.
    if (message._buffer instanceof Uint8Array) {
      arrayBuffer = message._buffer.buffer.slice(
        message._buffer.byteOffset,
        message._buffer.byteOffset + message._buffer.byteLength
      );
    } else if (message.payloadBytes instanceof Uint8Array) {
      arrayBuffer = message.payloadBytes.buffer.slice(
        message.payloadBytes.byteOffset,
        message.payloadBytes.byteOffset + message.payloadBytes.byteLength
      );
    } else {
      // Último recurso: leer byte a byte evitando el getter payloadString
      // Buscamos el Uint8Array interno recorriendo las propiedades del objeto
      let rawBuf = null;
      for (const key of Object.keys(message)) {
        const val = message[key];
        if (val instanceof Uint8Array && val.length > 0) {
          rawBuf = val;
          break;
        }
      }
      if (rawBuf) {
        arrayBuffer = rawBuf.buffer.slice(rawBuf.byteOffset, rawBuf.byteOffset + rawBuf.byteLength);
      } else {
        // Fallback final: charCodeAt (sólo funciona si Paho no lanzó excepción antes)
        const str = message.payloadString;
        const buf = new Uint8Array(str.length);
        for (let i = 0; i < str.length; i++) buf[i] = str.charCodeAt(i) & 0xFF;
        arrayBuffer = buf.buffer;
      }
    }
  } catch (e) {
    log(`Error leyendo buffer waveform: ${e.message}`, 'error');
    return;
  }

  if (!arrayBuffer || arrayBuffer.byteLength === 0) {
    log('Forma de onda inválida: payload vacío.', 'error');
    return;
  }

  try {
    const chunk = parseWaveformChunk(arrayBuffer);
    handleWaveformChunk(chunk);
  } catch (e) {
    log(`Error procesando chunk de forma de onda: ${e.message}`, 'error');
  }
}

window.requestWaveform = function () {
  if (!client || !connected) {
    log('No conectado. No se puede solicitar forma de onda.', 'error');
    return;
  }

  const message = new Paho.MQTT.Message('1');
  message.destinationName = WAVEFORM_REQUEST_TOPIC;
  client.send(message);

  log(`→ Solicitud de forma de onda enviada a "${WAVEFORM_REQUEST_TOPIC}"`, 'success');

  const statusEl = $('waveformStatus');
  if (statusEl) {
    statusEl.textContent = 'Solicitud enviada, esperando chunks...';
  }
};

window.requestHarmonics = function () {
  if (!client || !connected) {
    log('No conectado. No se pueden solicitar armónicos.', 'error');
    return;
  }

  const message = new Paho.MQTT.Message('1');
  message.destinationName = HARMONICS_REQUEST_TOPIC;
  client.send(message);

  log(`→ Solicitud de armónicos enviada a "${HARMONICS_REQUEST_TOPIC}"`, 'success');
};

function initWaveformChart() {
  const canvas = $('waveformChart');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');

  waveformChart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [
        {
          label: 'Voltaje (V)',
          data: [],
          borderWidth: 2,
          tension: 0,
          pointRadius: 0,
          yAxisID: 'y'
        },
        {
          label: 'Corriente (A)',
          data: [],
          borderWidth: 2,
          tension: 0,
          pointRadius: 0,
          yAxisID: 'y1'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      interaction: {
        mode: 'nearest',
        intersect: false
      },
      plugins: {
        legend: {
          labels: {
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono'
            }
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              if (context.dataset.label.includes('Voltaje')) {
                return `${context.dataset.label}: ${context.parsed.y.toFixed(2)} V`;
              }

              return `${context.dataset.label}: ${context.parsed.y.toFixed(3)} A`;
            }
          }
        }
      },
      scales: {
        x: {
          type: 'linear',
          title: {
            display: true,
            text: 'Tiempo (ms)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            maxTicksLimit: 10
          },
          grid: {
            color: '#1e2d45'
          }
        },
        y: {
          type: 'linear',
          position: 'left',
          title: {
            display: true,
            text: 'Voltaje (V)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            callback: value => value + ' V'
          },
          grid: {
            color: '#1e2d45'
          }
        },
        y1: {
          type: 'linear',
          position: 'right',
          title: {
            display: true,
            text: 'Corriente (A)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            callback: value => value + ' A'
          },
          grid: {
            drawOnChartArea: false
          }
        }
      }
    }
  });
}

function processWaveformPayload(payload) {
  let voltage;
  let current;
  let labels;

  if (Array.isArray(payload.voltage) && Array.isArray(payload.current)) {
    voltage = payload.voltage;
    current = payload.current;
  } else if (Array.isArray(payload.v) && Array.isArray(payload.i)) {
    voltage = payload.v;
    current = payload.i;
  } else {
    log('Formato de forma de onda inválido. Se esperaba voltage/current o v/i.', 'error');
    return;
  }

  const sampleCount = Math.min(voltage.length, current.length);

  if (sampleCount === 0) {
    log('Forma de onda vacía.', 'error');
    return;
  }

  if (Array.isArray(payload.t)) {
    labels = payload.t.slice(0, sampleCount);
  } else if (Array.isArray(payload.time_ms)) {
    labels = payload.time_ms.slice(0, sampleCount);
  } else {
    labels = Array.from({ length: sampleCount }, (_, index) => index + 1);
  }

  const cleanVoltage = voltage.slice(0, sampleCount).map(Number);
  const cleanCurrent = current.slice(0, sampleCount).map(Number);

  if (
    cleanVoltage.some(value => !Number.isFinite(value)) ||
    cleanCurrent.some(value => !Number.isFinite(value))
  ) {
    log('Forma de onda inválida: voltaje y corriente deben ser numéricos.', 'error');
    return;
  }

  updateWaveformChart(labels, cleanVoltage, cleanCurrent);

  const statusEl = $('waveformStatus');
  if (statusEl) {
    statusEl.textContent = `Última captura: ${new Date().toLocaleTimeString('es-MX')} · ${sampleCount} muestras`;
  }
}

function parseWaveformChunk(arrayBuffer) {
  if (arrayBuffer.byteLength < WAVEFORM_HEADER_SIZE) {
    throw new Error(`Chunk demasiado corto: ${arrayBuffer.byteLength} bytes`);
  }

  const view = new DataView(arrayBuffer);

  // Leer header con los tipos correctos según el struct del ESP32:
  // uint32 sequence_id  @ offset 0
  // uint64 timestamp_ms @ offset 4
  // uint16 chunk_index  @ offset 12
  // uint16 chunk_count  @ offset 14
  // uint16 sample_rate  @ offset 16
  // uint16 total_samples@ offset 18
  // uint16 samples_in_chunk @ offset 20
  // uint8  channels     @ offset 22
  // uint8  format       @ offset 23
  const sequenceId     = view.getUint32(0, true);
  const chunkIndex     = view.getUint16(12, true);
  const chunkCount     = view.getUint16(14, true);
  const sampleRateHz   = view.getUint16(16, true);
  const totalSamples   = view.getUint16(18, true);
  const samplesInChunk = view.getUint16(20, true);
  const channels       = view.getUint8(22);
  const format         = view.getUint8(23);

  if (channels !== WAVEFORM_CHANNELS_EXPECTED) {
    throw new Error(`Canales no soportados: ${channels}`);
  }

  if (format !== WAVEFORM_FORMAT_I16_SCALED) {
    throw new Error(`Formato de waveform no soportado: ${format}`);
  }

  const expectedBytes = WAVEFORM_HEADER_SIZE + samplesInChunk * channels * 2;
  if (arrayBuffer.byteLength < expectedBytes) {
    throw new Error(`Chunk incompleto: recibidos ${arrayBuffer.byteLength} bytes, esperados ${expectedBytes}`);
  }

  const voltage = new Array(samplesInChunk);
  const current = new Array(samplesInChunk);

  let offset = WAVEFORM_HEADER_SIZE;
  for (let i = 0; i < samplesInChunk; i++) {
    voltage[i] = view.getInt16(offset, true) / VOLTAGE_SCALE;
    offset += 2;
    current[i] = view.getInt16(offset, true) / CURRENT_SCALE;
    offset += 2;
  }

  return {
    sequenceId,
    chunkIndex,
    chunkCount,
    sampleRateHz,
    totalSamples,
    samplesInChunk,
    voltage,
    current
  };
}

function handleWaveformChunk(chunk) {
  let sequence = waveformSequences.get(chunk.sequenceId);

  if (!sequence) {
    sequence = {
      sequenceId:   chunk.sequenceId,
      chunkCount:   chunk.chunkCount,
      receivedCount: 0,
      sampleRateHz: chunk.sampleRateHz,
      totalSamples: chunk.totalSamples,
      chunks:       new Array(chunk.chunkCount)
    };
    waveformSequences.set(chunk.sequenceId, sequence);

    if (waveformSequences.size > 8) {
      const oldestKey = waveformSequences.keys().next().value;
      waveformSequences.delete(oldestKey);
    }
  }

  if (!sequence.chunks[chunk.chunkIndex]) {
    sequence.chunks[chunk.chunkIndex] = chunk;
    sequence.receivedCount++;
  }

  const statusEl = $('waveformStatus');
  if (statusEl) {
    statusEl.textContent =
      `Recibiendo: ${sequence.receivedCount}/${sequence.chunkCount} chunks · ${chunk.totalSamples} muestras`;
  }

  log(`Chunk waveform recibido seq=${chunk.sequenceId} ${chunk.chunkIndex + 1}/${chunk.chunkCount}`, 'data');

  if (sequence.receivedCount === sequence.chunkCount) {
    renderWaveformSequence(sequence);
    waveformSequences.delete(chunk.sequenceId);
  }
}

function renderWaveformSequence(sequence) {
  const voltagePoints = [];
  const currentPoints = [];

  let sampleIndex = 0;
  const dtMs = 1000.0 / sequence.sampleRateHz;

  for (let chunkIndex = 0; chunkIndex < sequence.chunkCount; chunkIndex++) {
    const chunk = sequence.chunks[chunkIndex];

    if (!chunk) {
      log(`No se puede renderizar seq=${sequence.sequenceId}; falta chunk ${chunkIndex}`, 'error');
      return;
    }

    for (let i = 0; i < chunk.samplesInChunk; i++) {
      const tMs = sampleIndex * dtMs;
      voltagePoints.push({ x: tMs, y: chunk.voltage[i] });
      currentPoints.push({ x: tMs, y: chunk.current[i] });
      sampleIndex++;
    }
  }

  waveformChart.data.datasets[0].data = voltagePoints;
  waveformChart.data.datasets[1].data = currentPoints;

  // Ajustar escala del eje de corriente:
  // - Nunca menor a ±0.1 A (mínimo fijo)
  // - Se adapta automáticamente si la corriente real es mayor
  if (currentPoints.length > 0) {
    const rawMax = Math.max(...currentPoints.map(p => Math.abs(p.y)));
    const axisMax = Math.max(0.1, rawMax * 1.15);   // +15 % de margen visual
    waveformChart.options.scales.y1.min = -axisMax;
    waveformChart.options.scales.y1.max =  axisMax;
  }

  waveformChart.update('none');

  const statusEl = $('waveformStatus');
  if (statusEl) {
    statusEl.textContent = `Captura completa: ${sampleIndex} muestras · ${(sampleIndex * dtMs).toFixed(1)} ms`;
  }

  log(`Waveform renderizado: ${sampleIndex} muestras · seq=${sequence.sequenceId}`, 'success');
}

function publish(payload) {
  if (!client || !connected) {
    log('No conectado. No se puede enviar el comando.', 'error');
    return false;
  }
  const topic = 'sec/datos/cmd';
  const message = new Paho.MQTT.Message(JSON.stringify(payload));
  message.destinationName = topic;
  client.send(message);
  log(`→ [${topic}] ${JSON.stringify(payload)}`, 'success');
  return true;
}

// ------------------------------------------------------------------
// Actualizar UI
// ------------------------------------------------------------------
async function saveAlert(alertData) {
  const record = {
    code:       alertData.code      || 'FAULT_UNKNOWN',
    label:      alertData.label     || 'Falla desconocida',
    severity:   alertData.severity  || 'warn',
    value:      alertData.value     ?? null,
    alert_time: alertData.timestamp ?? new Date().toLocaleString('es-MX')
  };

  // Siempre registrar en memoria primero — garantiza que aparezca en el historial
  // aunque Supabase falle o la columna no exista
  localAlertLog.unshift({ time: record.alert_time, label: record.label, severity: record.severity });
  if (localAlertLog.length > 200) localAlertLog.pop();   // máx. 200 entradas

  // Actualizar historial visible inmediatamente (sin esperar a la DB)
  renderPowerAlerts();

  try {
    const { error } = await db
      .from('alerts')
      .insert(record);

    if (error) {
      // Si la columna alert_time no existe en la tabla, reintentar sin ella
      if (error.message && error.message.includes('alert_time')) {
        const { error: error2 } = await db
          .from('alerts')
          .insert({ code: record.code, label: record.label, severity: record.severity, value: record.value });
        if (error2) {
          log(`Error guardando alerta DB: ${error2.message}`, 'error');
          return;
        }
      } else {
        log(`Error guardando alerta DB: ${error.message}`, 'error');
        return;
      }
    }

    log(`⚠ Alerta guardada: ${record.label}`, record.severity === 'error' ? 'error' : 'warn');
  } catch (e) {
    log(`Error DB alerta: ${e.message}`, 'error');
  }
}

function processFaultFlags(faultFlags) {
  const flags = Number(faultFlags);
  if (!Number.isFinite(flags)) return;

  // Mostrar alerta activa junto al botón de conectar
  showLatestAlert(flags);

  // Guardar en historial solo fallas nuevas
  const newFaults = flags & ~lastFaultFlags;

  if (newFaults === 0) {
    lastFaultFlags = flags;
    return;
  }

  Object.entries(FAULTS).forEach(([code, info]) => {
    const mask = 1 << info.bit;

    if ((newFaults & mask) !== 0) {
      saveAlert({
        code,
        label: info.label,
        severity: info.severity,
        value: flags
      });
    }
  });

  // Mostrar modal para las fallas nuevas
  const newFaultsList = Object.entries(FAULTS)
    .filter(([, info]) => (newFaults & (1 << info.bit)) !== 0)
    .map(([, info]) => ({ label: info.label, severity: info.severity }));
  showAlertModal(newFaultsList);

  lastFaultFlags = flags;
}

async function renderPowerAlerts() {
  const body = $('powerAlertsBody');
  if (!body) return;

  // --- Paso 1: mostrar log local inmediatamente ---
  // Esto garantiza visibilidad aunque la DB no esté disponible
  function renderFromLocal() {
    body.innerHTML = '';
    if (localAlertLog.length === 0) {
      const emptyLine = document.createElement('div');
      emptyLine.className = 'log-line log-info';
      emptyLine.textContent = 'No hay alertas registradas.';
      body.appendChild(emptyLine);
      return;
    }
    localAlertLog.forEach(alert => {
      const line = document.createElement('div');
      line.className = alert.severity === 'error' ? 'log-line log-error' : 'log-line log-warn';
      line.textContent = `[${alert.time}] ${alert.label}`;
      body.appendChild(line);
    });
  }

  renderFromLocal();

  // --- Paso 2: intentar cargar historial persistente desde Supabase ---
  try {
    const { data, error } = await db
      .from('alerts')
      .select('created_at, alert_time, label, severity')
      .order('created_at', { ascending: false })
      .limit(100);

    if (error || !data || data.length === 0) return;

    // Combinar: DB como base histórica + entradas locales que no están en DB todavía
    // Construir set de claves únicas de la DB para deduplicar
    const dbKeys = new Set(data.map(a => {
      const t = a.alert_time || new Date(a.created_at).toLocaleString('es-MX');
      return `${t}|${a.label}`;
    }));

    // Entradas locales que aún no están en DB (recién generadas en esta sesión)
    const onlyLocal = localAlertLog.filter(a => !dbKeys.has(`${a.time}|${a.label}`));

    // Unir: primero las locales recientes, luego el histórico de DB
    const combined = [
      ...onlyLocal.map(a => ({ time: a.time, label: a.label, severity: a.severity })),
      ...data.map(a => ({
        time: a.alert_time || new Date(a.created_at).toLocaleString('es-MX'),
        label: a.label,
        severity: a.severity
      }))
    ];

    if (combined.length === 0) return;

    body.innerHTML = '';
    combined.forEach(alert => {
      const line = document.createElement('div');
      line.className = alert.severity === 'error' ? 'log-line log-error' : 'log-line log-warn';
      line.textContent = `[${alert.time}] ${alert.label}`;
      body.appendChild(line);
    });

  } catch (e) {
    // Si falla la DB, el log local ya está visible — no sobreescribir
    log(`Error leyendo historial de alertas DB: ${e.message}`, 'error');
  }
}

window.clearPowerAlerts = async function () {
  // Limpiar también el log local en memoria
  localAlertLog.length = 0;

  const { error } = await db
    .from('alerts')
    .delete()
    .neq('id', 0);

  if (error) {
    log(`Error limpiando alertas DB: ${error.message}`, 'error');
    return;
  }

  renderPowerAlerts();
  log('Historial de alertas limpiado en Supabase.', 'warn');
};

async function saveConsumptionPeriod() {
  if (!periodStartTime) return;

  const periodEndTime = new Date().toLocaleString('es-MX');
  const consumedKwh = Math.max(0, kwhTotal - periodStartKwh);

  try {
    const { error } = await db
      .from('consumption_periods')
      .insert({
        start_time: periodStartTime,
        end_time: periodEndTime,
        energy_kwh: Number(consumedKwh.toFixed(4))
      });

    if (error) {
      log(`Error guardando periodo DB: ${error.message}`, 'error');
      return;
    }

    log(`Periodo guardado en Supabase: ${consumedKwh.toFixed(4)} kWh`, 'success');

    renderConsumptionPeriods();

    periodStartTime = null;
    periodStartKwh = kwhTotal;
  } catch (e) {
    log(`Error DB periodo: ${e.message}`, 'error');
  }
}

async function renderConsumptionPeriods() {
  const body = $('consumptionPeriodsBody');
  if (!body) return;

  body.innerHTML = '';

  const { data, error } = await db
    .from('consumption_periods')
    .select('start_time, end_time, energy_kwh')
    .order('created_at', { ascending: false })
    .limit(20);

  if (error) {
    const line = document.createElement('div');
    line.className = 'log-line log-error';
    line.textContent = `Error leyendo periodos DB: ${error.message}`;
    body.appendChild(line);
    return;
  }

  if (!data || data.length === 0) {
    const emptyLine = document.createElement('div');
    emptyLine.className = 'log-line log-info';
    emptyLine.textContent = 'No hay periodos registrados.';
    body.appendChild(emptyLine);
    return;
  }

  data.forEach(period => {
    const line = document.createElement('div');
    line.className = 'log-line log-success';
    line.textContent = `[${period.start_time}] → [${period.end_time}] | ${Number(period.energy_kwh).toFixed(4)} kWh`;
    body.appendChild(line);
  });
}

window.clearConsumptionPeriods = async function () {
  const { error } = await db
    .from('consumption_periods')
    .delete()
    .neq('id', 0);

  if (error) {
    log(`Error limpiando periodos DB: ${error.message}`, 'error');
    return;
  }

  renderConsumptionPeriods();
  log('Historial de periodos limpiado en Supabase.', 'warn');
};

async function saveConsumptionPoint(deltaKwh) {
  pendingConsumptionKwh += deltaKwh;

  const now = Date.now();
  const elapsedMs = now - lastConsumptionSaveTime;

  // Guardar cada 60 segundos
  if (elapsedMs < 60000) return;

  const costResult = calculateTieredEnergyCost(kwhTotal);

  try {
    const { error } = await db
      .from('consumption_points')
      .insert({
        energy_kwh: Number(pendingConsumptionKwh.toFixed(6)),
        session_kwh: Number(kwhTotal.toFixed(6)),
        estimated_cost_mxn: Number(costResult.totalCost.toFixed(2))
      });

    if (error) {
      log(`Error guardando consumo DB: ${error.message}`, 'error');
      return;
    }

    pendingConsumptionKwh = 0;
    lastConsumptionSaveTime = now;

    renderConsumptionChartByRange(consumptionChartRange);
    updateEnergyCost();
  } catch (e) {
    log(`Error DB consumo: ${e.message}`, 'error');
  }
}

function initHarmonicChart() {
  const canvas = $('harmonicChart');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');

  harmonicChartLabels.length = 0;
  harmonicChartData.length = 0;

  for (let i = 1; i <= HARMONICS_COUNT; i++) {
    harmonicChartLabels.push(`H${i}`);
    harmonicChartData.push(0);
  }

  harmonicChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: harmonicChartLabels,
      datasets: [
        {
          label: 'Magnitud armónica (%)',
          data: harmonicChartData,
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: {
          labels: {
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono'
            }
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              return `${context.parsed.y.toFixed(2)} %`;
            }
          }
        }
      },
      scales: {
        x: {
          title: {
            display: true,
            text: 'Armónico',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            maxRotation: 0,
            autoSkip: false
          },
          grid: {
            color: '#1e2d45'
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Magnitud (%)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            callback: function(value) {
              return value + ' %';
            }
          },
          grid: {
            color: '#1e2d45'
          }
        }
      }
    }
  });
}

function processHarmonicsPayload(payload) {
  const harmonics = payload.current_harmonics || payload.harmonics || payload.armonicos || [];

  if (!Array.isArray(harmonics)) {
    log('Formato de armónicos inválido: no contiene arreglo de armónicos.', 'error');
    return;
  }

  const normalized = harmonics.slice(0, HARMONICS_COUNT).map((item, index) => {
    if (typeof item === 'number') {
      return {
        n: index + 1,
        rms: null,
        percent: Number(item)
      };
    }

    return {
      n: Number(item.n ?? item.order ?? item.harmonic ?? index + 1),
      rms: item.rms === undefined || item.rms === null ? null : Number(item.rms),
      percent: Number(item.percent ?? item.percentage ?? item.percent_of_fundamental ?? 0)
    };
  });

  if (normalized.some(item => !Number.isFinite(item.percent))) {
    log('Armónicos inválidos: el porcentaje debe ser numérico.', 'error');
    return;
  }

  updateHarmonicChart(normalized.map(item => item.percent));

  const lastUpdateEl = $('harmonicLastUpdate');
  if (lastUpdateEl) {
    const thd = Number(payload.thd ?? payload.current_thd_percent ?? 0);
    const fundamental = Number(payload.fundamental_hz ?? payload.fundamentalHz ?? 0);

    let text = new Date().toLocaleTimeString('es-MX');

    if (Number.isFinite(thd) && thd > 0) {
      text += ` · THD ${thd.toFixed(2)}%`;
    }

    if (Number.isFinite(fundamental) && fundamental > 0) {
      text += ` · ${fundamental.toFixed(2)} Hz`;
    }

    lastUpdateEl.textContent = text;
  }

  log(`Armónicos actualizados: ${normalized.length} valores`, 'success');
}

function updateHarmonicChart(harmonics) {
  if (!harmonicChart) return;

  harmonicChartData.length = 0;

  harmonics.forEach(value => {
    harmonicChartData.push(Number(value));
  });

  harmonicChart.update();

  const lastUpdateEl = $('harmonicLastUpdate');
  if (lastUpdateEl) {
    lastUpdateEl.textContent = new Date().toLocaleTimeString('es-MX');
  }
}

function calculateTieredEnergyCost(kwh) {
  let remainingKwh = Math.max(0, Number(kwh) || 0);
  let totalCost = 0;
  const details = [];

  CFE_TARIFF_1C.blocks.forEach(block => {
    if (remainingKwh <= 0) return;

    const blockKwh = Math.min(remainingKwh, block.limitKwh);
    const blockCost = blockKwh * block.price;

    details.push({
      label: block.label,
      kwh: blockKwh,
      price: block.price,
      cost: blockCost
    });

    totalCost += blockCost;
    remainingKwh -= blockKwh;
  });

  return {
    totalCost,
    details,
    isDacRisk: kwh >= CFE_TARIFF_1C.dacLimitKwhPerMonth
  };
}

async function updateEnergyCost() {
  const startIso = getIsoStartForRange('bimester');

  try {
    const { data, error } = await db
      .from('consumption_points')
      .select('energy_kwh')
      .gte('created_at', startIso);

    if (error) {
      log(`Error leyendo consumo bimestral DB: ${error.message}`, 'error');
      return;
    }

    const bimesterKwh = (data || []).reduce((total, item) => {
      return total + Number(item.energy_kwh || 0);
    }, 0);

    const result = calculateTieredEnergyCost(bimesterKwh);

    const costEl = $('val-cost');
    const costTariffEl = $('cost-tariff');
    const costBlockEl = $('cost-block');
    const costBreakdownEl = $('cost-breakdown');
    const dacWarningEl = $('dac-warning');

    if (costEl) {
      costEl.textContent = result.totalCost.toFixed(2);
    }

    if (costTariffEl) {
      costTariffEl.textContent = CFE_TARIFF_1C.name;
    }

    if (costBlockEl) {
      const lastBlock = result.details[result.details.length - 1];
      costBlockEl.textContent = lastBlock ? lastBlock.label : 'Sin consumo';
    }

    if (costBreakdownEl) {
      if (result.details.length === 0) {
        costBreakdownEl.textContent = 'Sin consumo registrado en el bimestre.';
      } else {
        costBreakdownEl.innerHTML = result.details
          .map(item => {
            return `${item.label}: ${item.kwh.toFixed(2)} kWh × $${item.price.toFixed(4)} = $${item.cost.toFixed(2)}`;
          })
          .join('<br>');
      }
    }

    if (dacWarningEl) {
      if (result.isDacRisk) {
        dacWarningEl.textContent = '⚠ Riesgo DAC: consumo bimestral elevado';
        dacWarningEl.classList.remove('hidden');
      } else {
        dacWarningEl.textContent = '';
        dacWarningEl.classList.add('hidden');
      }
    }

  } catch (e) {
    log(`Error costo bimestral: ${e.message}`, 'error');
  }
}

function initPowerChart() {
  const canvas = $('powerChart');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');

  powerChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: powerChartLabels,
      datasets: [
        {
          label: 'Potencia activa (W)',
          data: powerChartData,
          borderWidth: 2,
          tension: 0.35,
          pointRadius: 2
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: {
          labels: {
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono'
            }
          }
        },
        tooltip: {
          mode: 'index',
          intersect: false
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#4a6080',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8
          },
          grid: {
            color: '#1e2d45'
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Potencia activa (W)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            callback: function(value) {
              return value + ' W';
            }
          },
          grid: {
            color: '#1e2d45'
          }
        }
      }
    }
  });
}

function updatePowerChart(powerValue) {
  const now = new Date();

  lastPowerW = Number(powerValue);
  const kwhPowerEl = $('kwh-power');
  if (kwhPowerEl) kwhPowerEl.textContent = lastPowerW.toFixed(1) + ' W';

  if (!powerChart) return;

  // Siempre acumular en memoria (buffer de la última hora)
  // Esto garantiza que los datos en tiempo real no se pierdan al cambiar de pestaña
  const label = now.toLocaleTimeString('es-MX');
  powerChartLabels.push(label);
  powerChartData.push(Number(lastPowerW.toFixed(2)));

  while (powerChartLabels.length > 3600) {   // máx. 1 h a 1 s/muestra
    powerChartLabels.shift();
    powerChartData.shift();
  }

  // Solo actualizar la gráfica visualmente si estamos en la pestaña "hora"
  // Las otras pestañas se recargan desde DB únicamente cuando el usuario las selecciona
  if (powerChartRange === 'hour') {
    // Mostrar solo los últimos 60 puntos
    const visibleLabels = powerChartLabels.slice(-60);
    const visibleData   = powerChartData.slice(-60);

    powerChart.data.labels = visibleLabels;
    powerChart.data.datasets[0].data = visibleData;
    powerChart.update('none');
  }
}

function getIsoStartForRange(range) {
  const startTime = getStartTimeForRange(range);
  return new Date(startTime).toISOString();
}

async function fetchPowerHistoryFromDatabase(range) {
  const startIso = getIsoStartForRange(range);

  const { data, error } = await db
    .from('telemetry')
    .select('created_at, active_power')
    .gte('created_at', startIso)
    .not('active_power', 'is', null)
    .order('created_at', { ascending: true });

  if (error) {
    log(`Error leyendo potencia DB: ${error.message}`, 'error');
    return [];
  }

  return data || [];
}

function averageDbByGroup(data, getGroupKey, getLabel, valueKey) {
  const groups = {};

  data.forEach(item => {
    const date = new Date(item.created_at);
    const key = getGroupKey(date);

    if (!groups[key]) {
      groups[key] = {
        total: 0,
        count: 0,
        label: getLabel(date)
      };
    }

    groups[key].total += Number(item[valueKey]);
    groups[key].count += 1;
  });

  return Object.values(groups).map(group => ({
    label: group.label,
    value: group.total / group.count
  }));
}


async function renderPowerChartByRange(range = 'hour') {
  if (!powerChart) return;

  powerChartRange = range;

  // Pestaña "hora": usar el buffer en memoria (datos en tiempo real acumulados)
  // No leer DB — así los puntos en vivo nunca se pierden al cambiar de pestaña
  if (range === 'hour') {
    const visibleLabels = powerChartLabels.slice(-60);
    const visibleData   = powerChartData.slice(-60);

    powerChart.data.labels = visibleLabels;
    powerChart.data.datasets[0].data = visibleData;
    powerChart.update('none');
    updatePowerChartButtons(range);
    return;
  }

  // Pestañas día / semana / bimestre: cargar histórico desde Supabase
  const rawData = await fetchPowerHistoryFromDatabase(range);

  let chartData;

  if (range === 'day') {
    chartData = averageDbByGroup(
      rawData,
      date => `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}-${date.getHours()}`,
      date => `${String(date.getHours()).padStart(2, '0')}:00`,
      'active_power'
    );

  } else if (range === 'week') {
    chartData = averageDbByGroup(
      rawData,
      date => `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`,
      date => date.toLocaleDateString('es-MX', {
        weekday: 'short',
        day: '2-digit',
        month: '2-digit'
      }),
      'active_power'
    );

  } else if (range === 'bimester') {
    const startTime = getStartTimeForRange('bimester');

    chartData = averageDbByGroup(
      rawData,
      date => {
        const weekIndex = Math.floor((date.getTime() - startTime) / (7 * 24 * 60 * 60 * 1000));
        return `week-${weekIndex}`;
      },
      date => {
        const weekIndex = Math.floor((date.getTime() - startTime) / (7 * 24 * 60 * 60 * 1000));
        return `Sem ${weekIndex + 1}`;
      },
      'active_power'
    );
  }

  // Para estas vistas históricas usamos arrays temporales en la gráfica
  // SIN tocar powerChartLabels/powerChartData (que son el buffer en vivo)
  powerChart.data.labels = chartData.map(item => item.label);
  powerChart.data.datasets[0].data = chartData.map(item => Number(item.value.toFixed(2)));

  powerChart.update('none');
  updatePowerChartButtons(range);
}

function updatePowerChartButtons(activeRange) {
  // Acotar al primer panel de gráfica (potencia activa) para no afectar
  // los botones de consumo que comparten la clase chart-range-btn
  const powerPanel = document.querySelector('#powerChart')?.closest('.chart-panel');
  const buttons = powerPanel
    ? powerPanel.querySelectorAll('.chart-range-btn')
    : document.querySelectorAll('.chart-range-btn[data-range]');   // fallback seguro

  buttons.forEach(btn => {
    if (btn.dataset.range === activeRange) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });
}

window.setPowerChartRange = function (range) {
  renderPowerChartByRange(range);
};

function initConsumptionChart() {
  const canvas = $('consumptionChart');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');

  consumptionChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: consumptionChartLabels,
      datasets: [
        {
          label: 'Consumo de energía (kWh)',
          data: consumptionChartData,
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: {
          labels: {
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono'
            }
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              return `${context.parsed.y.toFixed(4)} kWh`;
            }
          }
        }
      },
      scales: {
        x: {
          title: {
            display: true,
            text: 'Periodo',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8
          },
          grid: {
            color: '#1e2d45'
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Consumo de energía (kWh)',
            color: '#c8d8f0',
            font: {
              family: 'Share Tech Mono',
              size: 12
            }
          },
          ticks: {
            color: '#4a6080',
            callback: function(value) {
              return value + ' kWh';
            }
          },
          grid: {
            color: '#1e2d45'
          }
        }
      }
    }
  });
}

async function fetchConsumptionHistoryFromDatabase(range) {
  const startIso = getIsoStartForRange(range);

  const { data, error } = await db
    .from('consumption_points')
    .select('created_at, energy_kwh, session_kwh, estimated_cost_mxn')
    .gte('created_at', startIso)
    .order('created_at', { ascending: true });

  if (error) {
    log(`Error leyendo consumo DB: ${error.message}`, 'error');
    return [];
  }

  return data || [];
}

function sumDbByGroup(data, getGroupKey, getLabel, valueKey) {
  const groups = {};

  data.forEach(item => {
    const date = new Date(item.created_at);
    const key = getGroupKey(date);

    if (!groups[key]) {
      groups[key] = {
        total: 0,
        label: getLabel(date)
      };
    }

    groups[key].total += Number(item[valueKey]);
  });

  return Object.values(groups).map(group => ({
    label: group.label,
    value: group.total
  }));
}

async function renderConsumptionChartByRange(range = 'week') {
  if (!consumptionChart) return;

  consumptionChartRange = range;

  const rawData = await fetchConsumptionHistoryFromDatabase(range);

  let chartData;

  if (range === 'hour') {
    chartData = sumDbByGroup(
      rawData,
      date => {
        const minutesBlock = Math.floor(date.getMinutes() / 5) * 5;
        return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}-${date.getHours()}-${minutesBlock}`;
      },
      date => {
        const minutesBlock = Math.floor(date.getMinutes() / 5) * 5;
        return `${String(date.getHours()).padStart(2, '0')}:${String(minutesBlock).padStart(2, '0')}`;
      },
      'energy_kwh'
    );

  } else if (range === 'day') {
    chartData = sumDbByGroup(
      rawData,
      date => `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}-${date.getHours()}`,
      date => `${String(date.getHours()).padStart(2, '0')}:00`,
      'energy_kwh'
    );

  } else if (range === 'week') {
    chartData = sumDbByGroup(
      rawData,
      date => `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`,
      date => date.toLocaleDateString('es-MX', {
        weekday: 'short',
        day: '2-digit',
        month: '2-digit'
      }),
      'energy_kwh'
    );

  } else if (range === 'bimester') {
    const startTime = getStartTimeForRange('bimester');

    chartData = sumDbByGroup(
      rawData,
      date => {
        const weekIndex = Math.floor((date.getTime() - startTime) / (7 * 24 * 60 * 60 * 1000));
        return `week-${weekIndex}`;
      },
      date => {
        const weekIndex = Math.floor((date.getTime() - startTime) / (7 * 24 * 60 * 60 * 1000));
        return `Sem ${weekIndex + 1}`;
      },
      'energy_kwh'
    );
  }

  consumptionChartLabels.length = 0;
  consumptionChartData.length = 0;

  chartData.forEach(item => {
    consumptionChartLabels.push(item.label);
    consumptionChartData.push(Number(item.value.toFixed(4)));
  });

  consumptionChart.update();
  updateConsumptionChartButtons(range);
}

function updateConsumptionChartButtons(activeRange) {
  const buttons = document.querySelectorAll('.consumption-range-btn');

  buttons.forEach(btn => {
    if (btn.dataset.range === activeRange) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });
}

window.setConsumptionChartRange = function (range) {
  renderConsumptionChartByRange(range);
};


function updateDashboard(d) {
  if (d.v !== undefined) {
    setVal('v', d.v, 1, 'V');
    setBar('bar-v', d.v, LIMITS.v);
  }
  if (d.i !== undefined) {
    setVal('i', d.i, 2, 'A');
    setBar('bar-i', d.i, LIMITS.i);
  }
  if (d.p_activa !== undefined) {
    setVal('pa', d.p_activa, 1, 'W');
    lastPowerW = parseFloat(d.p_activa);
    const _kwhPow1 = $('kwh-power'); if (_kwhPow1) _kwhPow1.textContent = lastPowerW.toFixed(1) + ' W';

    updatePowerChart(lastPowerW);
  }
  if (d.p_aparente !== undefined) setVal('pap', d.p_aparente, 1, 'VA');
  if (d.p_reactiva !== undefined) setVal('pr',  d.p_reactiva, 1, 'VAR');
  if (d.fp         !== undefined) { setVal('fp', d.fp, 2, ''); updateFpArc(d.fp); }
  if (d.thd        !== undefined) { setVal('thd', d.thd, 1, '%'); updateThdBars(d.thd); }
  if (d.fault_flags !== undefined) {
    processFaultFlags(d.fault_flags);
  }
}

// ------------------------------------------------------------------
// Helpers de actualización de tarjetas
// ------------------------------------------------------------------
function setVal(key, value, decimals, unit) {
  const el = els[key];
  if (!el) return;
  el.textContent = parseFloat(value).toFixed(decimals);
  flash(el);
  const unitEl = el.parentElement.querySelector('.card-unit');
  if (unitEl && unit) unitEl.textContent = unit;
}

function setBar(barId, value, max) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  $(barId).style.width = pct + '%';
}

function updateFpArc(fp) {
  const arcLen = 173;
  const filled = Math.min(1, Math.max(0, fp)) * arcLen;
  const arcEl  = $('fp-arc-fill');
  arcEl.setAttribute('stroke-dasharray', `${filled} ${arcLen}`);

  let color, label;
  if      (fp >= 0.95) { color = '#39ff14'; label = 'EXCELENTE'; }
  else if (fp >= 0.85) { color = '#00e5ff'; label = 'BUENO'; }
  else if (fp >= 0.70) { color = '#ffcc00'; label = 'REGULAR'; }
  else                 { color = '#ff6b35'; label = 'BAJO'; }

  arcEl.setAttribute('stroke', color);
  $('fp-rating').textContent = label;
  $('fp-rating').style.color = color;
}

function updateThdBars(thd) {
  const bars = document.querySelectorAll('.hbar');
  const harmonics = [100, thd * 6, thd * 4, thd * 2.5, thd * 1.5, thd, thd * 0.5];
  bars.forEach((bar, i) => {
    bar.style.height = Math.min(100, harmonics[i] || 2) + '%';
  });
}

function flash(el) {
  el.classList.remove('updated');
  void el.offsetWidth;
  el.classList.add('updated');
}

// ------------------------------------------------------------------
// Acumulador kWh
// El timer corre al mismo ritmo que el muestreo del ESP32 para evitar
// oscilaciones cuando el intervalo es mayor a 1 segundo.
// ------------------------------------------------------------------
let kwhSampleMs = 1000;   // Intervalo de muestreo actual en ms (se actualiza con sendSampleRate)

function startKwhTimer() {
  if (kwhTimerInterval) {
    clearInterval(kwhTimerInterval);
  }

  kwhStartTime = Date.now();
  periodStartTime = new Date().toLocaleString('es-MX');
  periodStartKwh = kwhTotal;

  _scheduleKwhTick();
}

function _scheduleKwhTick() {
  if (kwhTimerInterval) clearInterval(kwhTimerInterval);

  kwhTimerInterval = setInterval(() => {
    // Acumular energía proporcional al intervalo real (no siempre 1 s)
    const deltaKwh = (lastPowerW * kwhSampleMs) / 3_600_000_000;

    kwhTotal += deltaKwh;

    saveConsumptionPoint(deltaKwh);

    $('val-kwh').textContent = kwhTotal.toFixed(4);
    $('kwh-session').textContent = kwhTotal.toFixed(4) + ' kWh';
    const _kwhPow2 = $('kwh-power'); if (_kwhPow2) _kwhPow2.textContent = lastPowerW.toFixed(1) + ' W';

    // Actualizar costo de sesión en pesos
    const sessionCost = calculateTieredEnergyCost(kwhTotal);
    const costSessionEl = $('cost-session');
    if (costSessionEl) costSessionEl.textContent = '$' + sessionCost.totalCost.toFixed(4) + ' MXN';

    flash(els.kwh);

    const elapsed = Math.floor((Date.now() - kwhStartTime) / 1000);
    const h = String(Math.floor(elapsed / 3600)).padStart(2, '0');
    const m = String(Math.floor((elapsed % 3600) / 60)).padStart(2, '0');
    const s = String(elapsed % 60).padStart(2, '0');

    $('kwh-time').textContent = `${h}:${m}:${s}`;
  }, kwhSampleMs);
}

// Llamar esto cuando el usuario cambia el tiempo de muestreo
// (después de sendSampleRate) para sincronizar el timer y el watchdog
function applySampleRate(segundos) {
  const s = Math.max(1, parseInt(segundos) || 1);
  kwhSampleMs = s * 1000;

  // Reiniciar el timer con el nuevo intervalo si ya está corriendo
  if (kwhTimerInterval) {
    _scheduleKwhTick();
    log(`Frecuencia de actualización ajustada a ${s} s`, 'info');
  }

  // El watchdog dinámico se actualiza automáticamente en el siguiente mensaje
  // recibido desde el ESP32 (usa sampleRate.value en tiempo real)
}

window.resetKwh = function () {
  // Antes de resetear, guardar el periodo actual
  if (periodStartTime && kwhTotal > periodStartKwh) {
    saveConsumptionPeriod();
  }

  // Reiniciar contador de energía
  kwhTotal = 0;
  kwhStartTime = Date.now();

  $('val-kwh').textContent = '0.0000';
  $('kwh-session').textContent = '0.0000 kWh';
  const _kwhPow3 = $('kwh-power'); if (_kwhPow3) _kwhPow3.textContent = lastPowerW.toFixed(1) + ' W';
  $('kwh-time').textContent = '00:00:00';
  updateEnergyCost();

  // Iniciar nuevo periodo después del reset
  periodStartTime = new Date().toLocaleString('es-MX');
  periodStartKwh = kwhTotal;

  log('Contador kWh reseteado y periodo guardado.', 'warn');
};

// ------------------------------------------------------------------
// Comandos del dispositivo
// ------------------------------------------------------------------

// Límite de potencia — sincronizar slider ↔ input
window.syncPowerLimit = function (val) {
  $('powerLimit').value = val;
};
window.updatePowerLimitDisplay = function () {
  let v = parseInt($('powerLimit').value);
  if (isNaN(v)) v = 0;
  v = Math.min(1200, Math.max(0, v));
  $('powerLimit').value       = v;
  $('powerLimitSlider').value = Math.min(1200, v);
};


// Tiempo de muestreo — sincronizar slider ↔ input
window.syncSampleRate = function (val) {
  $('sampleRate').value = val;
};
window.updateSampleDisplay = function () {
  let v = parseInt($('sampleRate').value);
  if (isNaN(v)) v = 1;
  v = Math.max(1, v);
  $('sampleRate').value   = v;
  $('sampleSlider').value = Math.min(60, v);
};


// ------------------------------------------------------------------
// Estado de conexión
// ------------------------------------------------------------------
function setStatus(isConnected) {
  const dot  = $('statusDot');
  const text = $('statusText');
  if (isConnected) {
    dot.classList.add('connected');
    text.textContent = 'Conectado';
  } else {
    dot.classList.remove('connected');
    text.textContent = 'Desconectado';
  }
}

// ------------------------------------------------------------------
// Log de consola
// ------------------------------------------------------------------
function log(msg, type = 'info') {
  const body = $('logBody');
  const ts   = new Date().toLocaleTimeString('es-MX');
  const line = document.createElement('div');
  line.className = `log-line log-${type}`;
  line.textContent = `[${ts}] ${msg}`;
  body.appendChild(line);
  body.scrollTop = body.scrollHeight;
  while (body.children.length > 100) body.removeChild(body.firstChild);
}

window.clearLog = function () {
  $('logBody').innerHTML = '';
  log('Log limpiado.', 'info');
};

// ------------------------------------------------------------------
// Simulador (consola del navegador: startSim() / stopSim())
// ------------------------------------------------------------------
let simInterval = null;

window.startSim = function (intervalMs = 1000) {
  stopSim();
  if (!kwhStartTime) startKwhTimer();
  log('▸ Simulador iniciado (sin broker).', 'warn');
  simInterval = setInterval(() => {
    const v   = +(120 + Math.random() * 10 - 5).toFixed(1);
    const i   = +(2  + Math.random() * 0.5).toFixed(2);
    const pa  = +(v * i * 0.9).toFixed(1);
    const pap = +(v * i).toFixed(1);
    const pr  = +(pa * 0.15).toFixed(1);
    const fp  = +(pa / pap).toFixed(2);
    const thd = +(4  + Math.random() * 1.5).toFixed(1);

    const payload = { v, i, p_activa: pa, p_aparente: pap, p_reactiva: pr, fp, thd };
    log(`[SIM] ${JSON.stringify(payload)}`, 'data');
    updateDashboard(payload);
    $('lastUpdate').textContent = new Date().toLocaleTimeString('es-MX');
  }, intervalMs);
};

window.stopSim = function () {
  if (simInterval) {
    clearInterval(simInterval);
    simInterval = null;
    log('■ Simulador detenido.', 'warn');
  }
};
// ============================================================
// FUNCIONES PARA ENVIAR COMANDOS (PUBLICAR EN MQTT)
// ============================================================

// Función genérica para enviar mensajes fácilmente
function publishMessage(topic, payload) {
  if (connected && client) {
    // Paho MQTT requiere crear un objeto Message
    const message = new Paho.MQTT.Message(payload.toString());
    message.destinationName = topic;
    client.send(message);
  } else {
    log('Error: No se puede enviar comando, el broker está desconectado.', 'error');
  }
}

window.toggleRelay = function () {
  // Evaluamos cómo creemos que está el relé para pedir lo contrario
  const payload = relayOn ? 'OFF' : 'ON';
  const topic = 'smartcontact/contacto_01/control/rele';

  // Mandamos la orden al hardware
  publishMessage(topic, payload);
  log(`▸ Comando enviado: Relé -> ${payload} (Esperando confirmación física...)`, 'info');

  // ⛔ NO cambiamos la variable relayOn ni los colores del botón aquí.
};

// 2. Comando de Límite de Potencia
window.sendPowerLimit = function () {
  const rawValue = $('powerLimitSlider').value;
  const limitValue = String(rawValue).padStart(4, '0');
  publishToAllDevices('control/limite_potencia', limitValue);
};

// 3. Comando de Tiempo de Muestreo
window.sendSampleRate = function () {
  const sampleValue = $('sampleRate').value;
  publishToAllDevices('control/tiempo_muestreo', sampleValue);
  applySampleRate(sampleValue);
};

// 4. Comando de comportamiento sin carga (FAULT_NO_LOAD)
// Control path: control/no_load_action
// Payload: "OFF"  → desconectar salida automáticamente cuando no hay corriente
//          "KEEP" → mantener salida encendida aunque no haya corriente
window.sendNoLoadAction = function (value) {
  const label = value === 'OFF' ? 'Desconectar salida sin carga' : 'Mantener salida sin carga';
  publishToAllDevices('control/no_load_action', value);
  log(`  (${label})`, 'info');
};

window.addEventListener('DOMContentLoaded', () => {
  initPowerChart();
  initConsumptionChart();
  initHarmonicChart();
  initWaveformChart();

  testDatabaseConnection();

  renderPowerChartByRange('hour');
  renderConsumptionChartByRange('week');

  renderPowerAlerts();
  renderConsumptionPeriods();

  updateEnergyCost();

  if (relayOn && !periodStartTime) {
    periodStartTime = new Date().toLocaleString('es-MX');
    periodStartKwh = kwhTotal;
  }
});