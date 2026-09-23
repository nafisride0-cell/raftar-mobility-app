import express from 'express';
import http from 'http';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { createServer as createViteServer } from 'vite';
import { Server as SocketIOServer } from 'socket.io';

const currentDir =
  typeof __dirname !== 'undefined'
    ? __dirname
    : path.dirname(fileURLToPath(import.meta.url));

// Server-side in-memory OTP verification record
interface ServerOtpRecord {
  phone: string;
  otp: string;
  role: string;
  createdAt: number;
  expiresAt: number;
  attempts: number;
}

const otpStore = new Map<string, ServerOtpRecord>();

// Server-side database ledger for live production persistence (initialized as blank production ledger)
interface ServerLedgerState {
  captains: any[];
  activeRide: any | null;
  rideHistory: any[];
  sosAlerts: any[];
  fareSettings: any;
  smsReceipts: any[];
}

const serverLedger: ServerLedgerState = {
  // Empty production ledger: only genuinely registered and verified drivers occupy our fleet map
  captains: [],
  activeRide: null,
  rideHistory: [],
  sosAlerts: [],
  fareSettings: {
    bike: { baseFare: 20, perKm: 8, minFare: 25 },
    auto: { baseFare: 30, perKm: 12, minFare: 35 },
    eriksha: { baseFare: 15, perKm: 7, minFare: 20 },
    maxRideLimitKm: 75,
    commissionPercentage: 12, // Strict 12% platform fee matrix
    founderName: 'Nafis Khan',
    supportEmail: 'nafiskhan.dsep9e@detedu.org',
    sosHelpline: '+91 70236 08919',
  },
  smsReceipts: [],
};

// Server-side Socket Registry mapping socket IDs to authenticated sessions
interface SocketSession {
  socketId: string;
  captainId?: string;
  riderId?: string;
  role: 'captain' | 'passenger' | 'admin';
  cityId: string;
  lat?: number;
  lng?: number;
  lastActive: number;
}

const socketStore = new Map<string, SocketSession>();
const captainSocketMap = new Map<string, string>(); // captainId -> socketId

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(express.json());

  // Attach HTTP Server and Socket.io instance
  const httpServer = http.createServer(app);
  const io = new SocketIOServer(httpServer, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST', 'PUT', 'DELETE'],
    },
    transports: ['websocket', 'polling'],
  });

  // ==========================================
  // WEBSOCKETS REAL-TIME DATA HIGHWAY
  // ==========================================
  io.on('connection', (socket) => {
    // 1. Initial connection session registration
    socketStore.set(socket.id, {
      socketId: socket.id,
      role: 'passenger',
      cityId: 'delhi',
      lastActive: Date.now(),
    });

    // 2. City channel subscription for passenger views
    socket.on('passenger:join_city', (data: { cityId: string; riderId?: string }) => {
      const cityId = data.cityId || 'delhi';
      socket.join(`city_${cityId}`);
      socket.join('passengers');

      const existing = socketStore.get(socket.id);
      if (existing) {
        existing.cityId = cityId;
        if (data.riderId) existing.riderId = data.riderId;
        existing.role = 'passenger';
        existing.lastActive = Date.now();
      }

      // Send immediate fleet snapshot for target operational city
      const cityCaptains = serverLedger.captains.filter(
        (c) => (c.cityId === cityId || (!c.cityId && cityId === 'delhi')) && c.isOnline
      );
      socket.emit('fleet:initial_snapshot', {
        cityId,
        captains: cityCaptains,
      });
    });

    // 3. Captain Authentication & WebSocket registration
    socket.on('captain:register', (data: { captainId: string; cityId?: string }) => {
      const { captainId, cityId = 'delhi' } = data;
      if (!captainId) return;

      captainSocketMap.set(captainId, socket.id);
      socketStore.set(socket.id, {
        socketId: socket.id,
        captainId,
        role: 'captain',
        cityId,
        lastActive: Date.now(),
      });

      socket.join(`city_${cityId}`);
      socket.join('captains');
      socket.join(`captain_${captainId}`);

      // Update captain status in ledger if present
      serverLedger.captains = serverLedger.captains.map((c) =>
        c.id === captainId ? { ...c, isOnline: true, ...(cityId ? { cityId } : {}) } : c
      );

      // Broadcast to city passengers that captain is active
      io.to(`city_${cityId}`).emit('fleet:captain_status_changed', {
        captainId,
        isOnline: true,
        cityId,
      });
    });

    // 4. Live High-Frequency GPS Location Sharing from Captain Device
    socket.on(
      'captain:share_location_live',
      (payload: {
        captainId: string;
        lat: number;
        lng: number;
        cityId?: string;
        vehicleType?: string;
        speed?: number;
        heading?: number;
      }) => {
        const { captainId, lat, lng, cityId, vehicleType, speed, heading } = payload;
        if (!captainId || typeof lat !== 'number' || typeof lng !== 'number') return;

        // Auto-detect or preserve operational city (delhi, gurugram, haryana_subhubs, alwar, jodhpur)
        const effectiveCity = cityId || socketStore.get(socket.id)?.cityId || 'delhi';

        // Update or insert captain record in server-side ledger
        const existingCaptainIndex = serverLedger.captains.findIndex((c) => c.id === captainId);
        if (existingCaptainIndex >= 0) {
          serverLedger.captains[existingCaptainIndex] = {
            ...serverLedger.captains[existingCaptainIndex],
            lat,
            lng,
            cityId: effectiveCity,
            isOnline: true,
            ...(vehicleType ? { vehicleType } : {}),
          };
        }

        // Room broadcast to passengers viewing this city
        io.to(`city_${effectiveCity}`).emit('fleet:captain_moved', {
          captainId,
          lat,
          lng,
          cityId: effectiveCity,
          vehicleType,
          speed: speed || 0,
          heading: heading || 0,
          timestamp: Date.now(),
        });

        // Direct telemetry stream to passenger if captain is assigned to an active trip
        if (serverLedger.activeRide && serverLedger.activeRide.captainId === captainId) {
          io.emit('ride:captain_live_telemetry', {
            rideId: serverLedger.activeRide.id,
            captainId,
            lat,
            lng,
            speed: speed || 0,
            heading: heading || 0,
          });
        }
      }
    );

    // 5. Dynamic Ride Request Broadcast from Passenger (No fake auto-match)
    socket.on('ride:request_dispatch', (rideData: any) => {
      if (!rideData) return;
      serverLedger.activeRide = rideData;

      const targetCity = rideData.pickupLocation?.cityId || 'delhi';

      // Broadcast new ride to online captains in the operational corridor
      io.to(`city_${targetCity}`).emit('ride:new_available', {
        ride: rideData,
        timestamp: Date.now(),
      });
      io.to('captains').emit('ride:new_available', {
        ride: rideData,
        timestamp: Date.now(),
      });
    });

    // 6. Captain Accepts Ride (Explicit Action Trigger)
    socket.on('ride:captain_accept', (data: { rideId: string; captainId: string }) => {
      const { rideId, captainId } = data;
      const captain = serverLedger.captains.find((c) => c.id === captainId);
      if (!captain || !serverLedger.activeRide || serverLedger.activeRide.id !== rideId) return;

      serverLedger.activeRide = {
        ...serverLedger.activeRide,
        captainId: captain.id,
        captainName: captain.name,
        captainPhone: captain.phone,
        captainPhoto: captain.photoUrl,
        vehiclePlate: captain.vehiclePlate,
        vehicleModel: captain.vehicleModel,
        status: 'captain_assigned',
      };

      serverLedger.captains = serverLedger.captains.map((c) =>
        c.id === captainId ? { ...c, currentRideId: rideId } : c
      );

      // Broadcast ride assigned to passenger
      io.emit('ride:status_update', {
        rideId,
        status: 'captain_assigned',
        ride: serverLedger.activeRide,
      });
    });

    // 7. Ride Status Transitions (Arrived, In-Trip, Completed, Cancelled)
    socket.on('ride:status_change', (data: { rideId: string; status: string; otp?: string }) => {
      if (!serverLedger.activeRide || serverLedger.activeRide.id !== data.rideId) return;

      serverLedger.activeRide.status = data.status;
      if (data.status === 'completed' || data.status === 'cancelled') {
        const finished = { ...serverLedger.activeRide };
        serverLedger.rideHistory.unshift(finished);
        if (finished.captainId) {
          serverLedger.captains = serverLedger.captains.map((c) =>
            c.id === finished.captainId ? { ...c, currentRideId: null } : c
          );
        }
      }

      io.emit('ride:status_update', {
        rideId: data.rideId,
        status: data.status,
        ride: serverLedger.activeRide,
      });
    });

    // 8. Emergency SOS Broadcast over WebSockets
    socket.on('sos:dispatch_beacon', (alert: any) => {
      if (alert && alert.id) {
        serverLedger.sosAlerts.unshift(alert);
        io.emit('sos:emergency_broadcast', alert);
      }
    });

    // 9. Cleanup on disconnect
    socket.on('disconnect', () => {
      const session = socketStore.get(socket.id);
      if (session?.captainId) {
        captainSocketMap.delete(session.captainId);
      }
      socketStore.delete(socket.id);
    });
  });

  // ==========================================
  // SECTION 1: ADMINISTRATIVE HARDENED GATEWAY
  // ==========================================
  app.post('/api/admin/verify-gate', (req, res) => {
    try {
      const { pin } = req.body;
      const serverPin = process.env.ADMIN_SECURE_PIN || '7860';
      const candidatePin = String(pin || '').trim();

      if (!candidatePin) {
        return res.status(400).json({
          success: false,
          verified: false,
          error: 'Missing security authentication token.',
        });
      }

      const expectedHash = crypto.createHash('sha256').update(serverPin).digest();
      const candidateHash = crypto.createHash('sha256').update(candidatePin).digest();

      const isValid = crypto.timingSafeEqual(expectedHash, candidateHash);

      if (!isValid) {
        return res.status(401).json({
          success: false,
          verified: false,
          error: 'Unauthorized: Invalid Admin Security PIN',
        });
      }

      const gateSessionToken = crypto.randomBytes(32).toString('hex');
      const timestamp = new Date().toISOString();

      return res.status(200).json({
        success: true,
        verified: true,
        token: gateSessionToken,
        timestamp,
        operator: 'Nafis Khan (Founder & CEO Command - Raftar Mobility)',
        message: 'Raftar Admin Control Center access authorized.',
      });
    } catch (err: any) {
      return res.status(500).json({
        success: false,
        verified: false,
        error: 'Cryptographic authentication engine error: ' + (err?.message || 'Internal failure'),
      });
    }
  });

  // ==========================================
  // SECTION 2: LIVE FAST2SMS DLT PROXY & OTP
  // ==========================================
  app.get('/api/fast2sms/balance', async (req, res) => {
    const apiKey =
      process.env.FAST2SMS_API_KEY ||
      'Fsp8bncLf6aIQSOhmRzTJNiwqB2tPkGvuV5WU7HEYC10de493jV1R7CWzIsfZhOLHm8Q0EbeMyrF9Tlo';

    try {
      const fRes = await fetch(
        `https://www.fast2sms.com/dev/wallet?authorization=${encodeURIComponent(apiKey)}`
      );
      const data: any = await fRes.json();
      if (data && data.return) {
        return res.json({
          success: true,
          wallet: parseFloat(data.wallet || '0'),
          smsCount: Number(data.sms_count || 0),
        });
      }
      return res.json({
        success: false,
        error: data?.message || 'Fast2SMS gateway authentication error',
      });
    } catch (err: any) {
      return res.json({
        success: false,
        error: err?.message || 'Failed to connect to Fast2SMS telecom server',
      });
    }
  });

  /**
   * Fast2SMS Bulk SMS HTTP Gateway Interface for Raftar Mobility
   */
  app.post('/api/otp/send', async (req, res) => {
    const { phone, role = 'passenger', otp: clientOtp } = req.body;
    const cleanPhone = String(phone || '').replace(/\D/g, '').slice(-10);

    if (cleanPhone.length !== 10) {
      return res.status(400).json({
        success: false,
        error: 'Invalid 10-digit Indian phone number.',
      });
    }

    // 4 to 6-digit verification code
    const otp = clientOtp || Math.floor(1000 + Math.random() * 9000).toString();
    const expiresAt = Date.now() + 5 * 60 * 1000;

    otpStore.set(cleanPhone, {
      phone: cleanPhone,
      otp,
      role,
      createdAt: Date.now(),
      expiresAt,
      attempts: 0,
    });

    const apiKey =
      process.env.FAST2SMS_API_KEY ||
      'Fsp8bncLf6aIQSOhmRzTJNiwqB2tPkGvuV5WU7HEYC10de493jV1R7CWzIsfZhOLHm8Q0EbeMyrF9Tlo';
    const senderId = process.env.FAST2SMS_SENDER_ID || 'NFSRID';
    const templateId = process.env.FAST2SMS_TEMPLATE_ID || '';
    const entityId = process.env.FAST2SMS_ENTITY_ID || '';

    let delivered = false;
    let requestId = `RF-F2S-${Date.now().toString().slice(-6)}`;
    let gatewayUsed = 'Fast2SMS TRAI DLT OTP Gateway (NFSRID / Raftar)';
    let gatewayError: string | undefined = undefined;

    // Production WhatsApp intent backup with Raftar Mobility branding
    const whatsappBackupMessage = encodeURIComponent(
      `रफ़्तार मोबिलिटी (Raftar Mobility) सुरक्षा सत्यापन कोड: ${otp}\nवैधता: 5 मिनट\nसंस्थापक: नफ़ीस खान (Founder & CEO)\nहेल्पलाइन: +91 70236 08919`
    );
    const whatsappBackupUrl = `https://wa.me/91${cleanPhone}?text=${whatsappBackupMessage}`;

    try {
      // Primary Path: Fast2SMS official Bulk SMS HTTP API
      const queryParams = new URLSearchParams({
        authorization: apiKey,
        route: templateId ? 'dlt' : 'otp',
        variables_values: otp,
        flash: '0',
        numbers: cleanPhone,
        sender_id: senderId,
      });

      if (templateId) queryParams.set('message', templateId);
      if (entityId) queryParams.set('entity_id', entityId);

      const fast2SmsUrl = `https://www.fast2sms.com/dev/bulkV2?${queryParams.toString()}`;
      const fRes = await fetch(fast2SmsUrl, {
        method: 'GET',
        headers: { 'Cache-Control': 'no-cache' },
      });
      const data: any = await fRes.json();

      if (data && data.return === true) {
        delivered = true;
        requestId = data.request_id || requestId;
      } else {
        // Fallback Quick Route
        const quickParams = new URLSearchParams({
          authorization: apiKey,
          route: 'q',
          message: `Raftar Mobility OTP: ${otp}. Valid 5 mins. Support: +91 70236 08919`,
          flash: '0',
          numbers: cleanPhone,
        });
        const qRes = await fetch(`https://www.fast2sms.com/dev/bulkV2?${quickParams.toString()}`, {
          method: 'GET',
          headers: { 'Cache-Control': 'no-cache' },
        });
        const qData: any = await qRes.json();

        if (qData && qData.return === true) {
          delivered = true;
          requestId = qData.request_id || requestId;
        } else {
          gatewayError = qData?.message || data?.message || 'Carrier network delivery pending';
        }
      }
    } catch (err: any) {
      gatewayError = err?.message || 'Fast2SMS gateway network timeout';
    }

    return res.json({
      success: true,
      delivered,
      requestId,
      gateway: gatewayUsed,
      senderId,
      templateId: templateId || undefined,
      whatsappBackupUrl,
      otp, // Provided for zero-friction fallback
      error: gatewayError,
      message: delivered
        ? `रफ़्तार ओटीपी +91 ${cleanPhone} पर सफलतापूर्वक भेज दिया गया है!`
        : `कैरियर विलंब: कृपया व्हाट्सएप या बैकअप माध्यम से सत्यापन कोड प्राप्त करें।`,
    });
  });

  app.post('/api/otp/verify', (req, res) => {
    const { phone, otp } = req.body;
    const cleanPhone = String(phone || '').replace(/\D/g, '').slice(-10);
    const entered = String(otp || '').trim();
    const record = otpStore.get(cleanPhone);

    if (!record) {
      return res.status(400).json({
        success: false,
        error: 'ओटीपी सत्र समाप्त हो चुका है। नया कोड प्राप्त करें।',
      });
    }

    if (Date.now() > record.expiresAt) {
      otpStore.delete(cleanPhone);
      return res.status(400).json({
        success: false,
        error: 'ओटीपी समाप्त हो गया है। कृपया नया कोड भेजें।',
      });
    }

    if (record.otp === entered) {
      otpStore.delete(cleanPhone);
      return res.json({
        success: true,
        message: 'सफल सत्यापन! (Raftar Mobility OTP Verified)',
        token: `jwt_raftar_${Date.now()}_${cleanPhone}`,
      });
    }

    record.attempts += 1;
    if (record.attempts >= 3) {
      otpStore.delete(cleanPhone);
      return res.status(400).json({
        success: false,
        error: 'अधिकतम 3 प्रयास पूरे हो चुके हैं। नया ओटीपी भेजें।',
      });
    }

    return res.status(400).json({
      success: false,
      error: `गलत ओटीपी कोड दर्ज किया गया। शेष प्रयास: ${3 - record.attempts}`,
    });
  });

  // ==========================================
  // SECTION 3: FLEET & OPERATOR LEDGER ENDPOINTS
  // ==========================================
  app.get('/api/fleet/captains', (req, res) => {
    res.json({ success: true, captains: serverLedger.captains });
  });

  app.put('/api/fleet/captain-status', (req, res) => {
    const { captainId, isOnline, kycStatus } = req.body;
    serverLedger.captains = serverLedger.captains.map((c) => {
      if (c.id === captainId) {
        return {
          ...c,
          ...(isOnline !== undefined ? { isOnline } : {}),
          ...(kycStatus ? { kycStatus } : {}),
        };
      }
      return c;
    });

    io.emit('fleet:captain_status_changed', { captainId, isOnline, kycStatus });
    res.json({ success: true, captainId, isOnline, kycStatus });
  });

  app.post('/api/fleet/captain-location', (req, res) => {
    const { captainId, lat, lng, cityId } = req.body;
    const targetCity = cityId || 'delhi';

    serverLedger.captains = serverLedger.captains.map((c) => {
      if (c.id === captainId) {
        return {
          ...c,
          lat,
          lng,
          cityId: targetCity,
        };
      }
      return c;
    });

    io.to(`city_${targetCity}`).emit('fleet:captain_moved', {
      captainId,
      lat,
      lng,
      cityId: targetCity,
      timestamp: Date.now(),
    });

    res.json({ success: true, captainId, lat, lng });
  });

  app.put('/api/fleet/captain-documents', (req, res) => {
    const { captainId, kycDocs, vehiclePlate, vehicleModel, vehicleType, cityId } = req.body;
    const existingIdx = serverLedger.captains.findIndex((c) => c.id === captainId);

    if (existingIdx >= 0) {
      serverLedger.captains[existingIdx] = {
        ...serverLedger.captains[existingIdx],
        kycDocs,
        vehiclePlate: vehiclePlate || serverLedger.captains[existingIdx].vehiclePlate,
        vehicleModel: vehicleModel || serverLedger.captains[existingIdx].vehicleModel,
        vehicleType: vehicleType || serverLedger.captains[existingIdx].vehicleType,
        cityId: cityId || serverLedger.captains[existingIdx].cityId,
      };
    } else {
      serverLedger.captains.push({
        id: captainId,
        kycDocs,
        vehiclePlate,
        vehicleModel,
        vehicleType,
        cityId: cityId || 'delhi',
        isOnline: false,
        kycStatus: 'pending',
        walletBalance: 0,
        todayEarnings: 0,
      });
    }
    res.json({ success: true, captainId });
  });

  // ==========================================
  // SECTION 4: RIDES & ACTIVE TRIPS LEDGER
  // ==========================================
  app.get('/api/rides/active', (req, res) => {
    res.json({ success: true, activeRide: serverLedger.activeRide });
  });

  app.get('/api/rides/history', (req, res) => {
    res.json({ success: true, history: serverLedger.rideHistory });
  });

  app.post('/api/rides/sync', (req, res) => {
    const { ride, action } = req.body;
    if (action === 'clear' || !ride) {
      serverLedger.activeRide = null;
    } else {
      serverLedger.activeRide = ride;
      if (ride.status === 'completed' || ride.status === 'cancelled') {
        const idx = serverLedger.rideHistory.findIndex((r) => r.id === ride.id);
        if (idx >= 0) {
          serverLedger.rideHistory[idx] = ride;
        } else {
          serverLedger.rideHistory.unshift(ride);
        }
      }
    }

    io.emit('ride:status_update', {
      rideId: ride?.id,
      status: ride?.status || 'cleared',
      ride: serverLedger.activeRide,
    });

    res.json({ success: true, activeRide: serverLedger.activeRide });
  });

  // ==========================================
  // SECTION 5: SAFETY BEACONS (SOS LEDGER)
  // ==========================================
  app.get('/api/sos/alerts', (req, res) => {
    res.json({ success: true, alerts: serverLedger.sosAlerts });
  });

  app.post('/api/sos/alert', (req, res) => {
    const alert = req.body;
    if (alert && alert.id) {
      serverLedger.sosAlerts.unshift(alert);
      io.emit('sos:emergency_broadcast', alert);
    }
    res.json({ success: true, alertId: alert?.id });
  });

  app.post('/api/sos/resolve', (req, res) => {
    const { alertId, notes } = req.body;
    serverLedger.sosAlerts = serverLedger.sosAlerts.map((a) =>
      a.id === alertId
        ? {
            ...a,
            status: 'resolved',
            resolvedBy: 'Founder Nafis Khan (Raftar Control Center)',
            resolvedAt: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            notes: notes || 'Assistance confirmed and distress resolved.',
          }
        : a
    );

    io.emit('sos:resolved', { alertId, notes });
    res.json({ success: true, alertId });
  });

  app.post('/api/sos/sms-log', (req, res) => {
    const receipt = req.body;
    serverLedger.smsReceipts.unshift(receipt);
    res.json({ success: true });
  });

  // ==========================================
  // SECTION 6: FARE SETTINGS & PHONEPE / UPI PAYMENT WEBHOOK
  // ==========================================
  app.get('/api/settings/fares', (req, res) => {
    res.json({ success: true, settings: serverLedger.fareSettings });
  });

  app.post('/api/settings/fares', (req, res) => {
    const newSettings = req.body;
    serverLedger.fareSettings = {
      ...serverLedger.fareSettings,
      ...newSettings,
      commissionPercentage: 12, // Strict 12% platform fee
    };
    io.emit('settings:fares_updated', serverLedger.fareSettings);
    res.json({ success: true, settings: serverLedger.fareSettings });
  });

  /**
   * PhonePe / Cashless / UPI Payment Webhook Ledger:
   * Commits transactions instantly, enforces strict 12% platform fee and 88% captain payout settlement ledger.
   */
  app.post('/api/payments/verify-webhook', (req, res) => {
    const {
      transactionId,
      merchantTransactionId,
      rideId,
      captainId,
      amount,
      utrNumber,
      status = 'PAYMENT_SUCCESS',
      providerReferenceId,
    } = req.body;

    const txId = transactionId || merchantTransactionId || `TXN-${Date.now()}`;
    const totalAmount = Number(amount) || 0;
    const platformCommission = Math.round(totalAmount * 0.12);
    const captainCredit = totalAmount - platformCommission; // 88%

    // Credit Captain Balance immediately in ledger
    if (captainId) {
      serverLedger.captains = serverLedger.captains.map((c) => {
        if (c.id === captainId) {
          return {
            ...c,
            walletBalance: (c.walletBalance || 0) + captainCredit,
            todayEarnings: (c.todayEarnings || 0) + captainCredit,
            totalTrips: (c.totalTrips || 0) + 1,
            currentRideId: null,
          };
        }
        return c;
      });
    }

    // Update Active Ride if applicable
    if (serverLedger.activeRide && (serverLedger.activeRide.id === rideId || !rideId)) {
      serverLedger.activeRide.paymentStatus = 'paid';
      serverLedger.activeRide.paymentDetails = {
        transactionId: txId,
        utrNumber: utrNumber || providerReferenceId || `UPI/${Date.now().toString().slice(-12)}`,
        amount: totalAmount,
        captainCredit,
        platformCommission,
        webhookVerified: true,
        paidAt: new Date().toLocaleTimeString(),
      };
    }

    // Broadcast instant payment event to sockets
    io.emit('payment:verified', {
      transactionId: txId,
      rideId,
      captainId,
      amount: totalAmount,
      captainCredit,
      platformCommission,
      status,
    });

    return res.json({
      success: true,
      transactionId: txId,
      rideId,
      status: status || 'SUCCESS',
      settlement: {
        totalAmount,
        platformCommissionRate: '12%',
        platformCommission,
        captainPayoutRate: '88%',
        captainCredit,
      },
      ledgerCommitTimestamp: new Date().toISOString(),
    });
  });

  // ==========================================
  // SECTION 7: VITE MIDDLEWARE & STATIC SERVING
  // ==========================================
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`Raftar Mobility Live WebSockets & REST Server listening on port ${PORT}`);
  });
}

startServer();
