import { connect } from 'cloudflare:sockets';

const UUID = new Uint8Array([
  0x55, 0xd9, 0xec, 0x38, 0x1b, 0x8a, 0x45, 0x4b,
  0x98, 0x1a, 0x6a, 0xcf, 0xe8, 0xf5, 0x6d, 0x8c
]);
const PROXY_HOST = 'sjc.o00o.ooo';
const PROXY_PORT = 443;

const WS_HI = 32768;
const WS_LO = 16384;
const MERGE_MAX = 12288;
const Q_SIZE = 32;
const Q_MASK = 31;
const QB_MAX = 262144;
const TIMEOUT = 1200;
const FAST_THRESHOLD = 4096;

const DEC = new TextDecoder();
const EMPTY = new Uint8Array(0);

const R400 = new Response(null, {status: 400});
const R403 = new Response(null, {status: 403});
const R426 = new Response(null, {status: 426, headers: {Upgrade: 'websocket'}});
const R502 = new Response(null, {status: 502});

function VLESSResult() {
  this.ok = false;
  this.host = '';
  this.port = 0;
  this.off = 0;
}

const VFAIL = Object.freeze(new VLESSResult());
const B64FAIL = EMPTY;

function b64dec(s) {
  let bin;
  try {
    bin = atob(s.replace(/-/g, '+').replace(/_/g, '/'));
  } catch {
    return B64FAIL;
  }
  
  const len = bin.length | 0;
  if (len === 0) return B64FAIL;
  
  const out = new Uint8Array(len);
  const end8 = (len & ~7) | 0;
  
  let i = 0;
  while (i < end8) {
    out[i] = bin.charCodeAt(i) | 0;
    out[i+1] = bin.charCodeAt(i+1) | 0;
    out[i+2] = bin.charCodeAt(i+2) | 0;
    out[i+3] = bin.charCodeAt(i+3) | 0;
    out[i+4] = bin.charCodeAt(i+4) | 0;
    out[i+5] = bin.charCodeAt(i+5) | 0;
    out[i+6] = bin.charCodeAt(i+6) | 0;
    out[i+7] = bin.charCodeAt(i+7) | 0;
    i = (i + 8) | 0;
  }
  while (i < len) {
    out[i] = bin.charCodeAt(i) | 0;
    i = (i + 1) | 0;
  }
  
  return out;
}

function chkUUID(d, o) {
  const o0 = o | 0;
  return (
    (((d[o0] ^ UUID[0]) | (d[o0+1] ^ UUID[1]) | (d[o0+2] ^ UUID[2]) | (d[o0+3] ^ UUID[3])) | 0) === 0 &&
    (((d[o0+4] ^ UUID[4]) | (d[o0+5] ^ UUID[5]) | (d[o0+6] ^ UUID[6]) | (d[o0+7] ^ UUID[7])) | 0) === 0 &&
    (((d[o0+8] ^ UUID[8]) | (d[o0+9] ^ UUID[9]) | (d[o0+10] ^ UUID[10]) | (d[o0+11] ^ UUID[11])) | 0) === 0 &&
    (((d[o0+12] ^ UUID[12]) | (d[o0+13] ^ UUID[13]) | (d[o0+14] ^ UUID[14]) | (d[o0+15] ^ UUID[15])) | 0) === 0
  );
}

function parseVL(d) {
  const len = d.length | 0;
  
  if (len < 22 || d[0] !== 0 || !chkUUID(d, 1)) return VFAIL;
  
  const alen = d[17] | 0;
  if (alen > 255) return VFAIL;
  
  const coff = (18 + alen) | 0;
  if ((coff + 3) > len || d[coff] !== 1) return VFAIL;
  
  const port = ((d[coff+1] << 8) | d[coff+2]) | 0;
  const aoff = (coff + 3) | 0;
  if (aoff >= len) return VFAIL;
  
  const atype = d[aoff] | 0;
  const r = new VLESSResult();
  r.ok = true;
  r.port = port;
  
  if (atype === 1) {
    const end = (aoff + 5) | 0;
    if (end > len) return VFAIL;
    r.host = `${d[aoff+1]}.${d[aoff+2]}.${d[aoff+3]}.${d[aoff+4]}`;
    r.off = end;
    return r;
  }
  
  if (atype === 2) {
    if ((aoff + 2) > len) return VFAIL;
    const dlen = d[aoff+1] | 0;
    const end = (aoff + 2 + dlen) | 0;
    if (end > len) return VFAIL;
    r.host = DEC.decode(d.subarray(aoff + 2, end));
    r.off = end;
    return r;
  }
  
  if (atype === 3) {
    const end = (aoff + 17) | 0;
    if (end > len) return VFAIL;
    const v = new DataView(d.buffer, d.byteOffset + aoff + 1, 16);
    const p0 = v.getUint16(0).toString(16);
    const p1 = v.getUint16(2).toString(16);
    const p2 = v.getUint16(4).toString(16);
    const p3 = v.getUint16(6).toString(16);
    const p4 = v.getUint16(8).toString(16);
    const p5 = v.getUint16(10).toString(16);
    const p6 = v.getUint16(12).toString(16);
    const p7 = v.getUint16(14).toString(16);
    r.host = p0 + ':' + p1 + ':' + p2 + ':' + p3 + ':' + p4 + ':' + p5 + ':' + p6 + ':' + p7;
    r.off = end;
    return r;
  }
  
  return VFAIL;
}

async function dial(host, port, fb) {
  const sock = connect({
    hostname: fb ? PROXY_HOST : host,
    port: (fb ? PROXY_PORT : port) | 0
  }, {allowHalfOpen: false});
  
  let tid = 0;
  try {
    await Promise.race([
      sock.opened,
      new Promise((_, rej) => { tid = setTimeout(rej, TIMEOUT) | 0; })
    ]);
  } finally {
    if (tid) clearTimeout(tid);
  }
  
  return sock;
}

function State(ws, tcp) {
  this.ws = ws;
  this.tcp = tcp;
  this.dead = false;
}

State.prototype.kill = function() {
  if (this.dead) return;
  this.dead = true;
  
  const ws = this.ws;
  const tcp = this.tcp;
  this.ws = null;
  this.tcp = null;
  
  queueMicrotask(() => {
    try { if (ws) ws.close(); } catch {}
    try { if (tcp) tcp.close(); } catch {}
  });
};

function Uplink(s, w) {
  this.s = s;
  this.w = w;
  this.q = new Array(Q_SIZE);
  this.qh = 0;
  this.qt = 0;
  this.qb = 0;
  this.lock = false;
  this.scheduled = false;
}

Uplink.prototype.push = function(chunk) {
  if (this.s.dead) return;
  
  const len = chunk.length | 0;
  const qh = this.qh | 0;
  const qt = this.qt | 0;
  const next = (qt + 1) & Q_MASK;
  const newQb = (this.qb + len) | 0;
  
  if (next === qh || newQb > QB_MAX) {
    this.s.kill();
    return;
  }
  
  this.q[qt] = chunk;
  this.qt = next;
  this.qb = newQb;
  
  const qsize = ((qt - qh + Q_SIZE) & Q_MASK) | 0;
  const isEmpty = qsize === 1;
  
  if (!this.lock && (isEmpty || len > FAST_THRESHOLD || newQb >= MERGE_MAX || qsize >= 10)) {
    this.drain();
  } else if (!this.scheduled) {
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      this.drain();
    });
  }
};

Uplink.prototype.drain = async function() {
  if (this.lock || this.s.dead || this.qh === this.qt) return;
  
  this.lock = true;
  const s = this.s;
  const w = this.w;
  const q = this.q;
  
  while (this.qh !== this.qt && !s.dead) {
    const qh = this.qh | 0;
    const qt = this.qt | 0;
    const qsize = ((qt - qh + Q_SIZE) & Q_MASK) | 0;
    
    let bc = 0;
    let bb = 0;
    
    while (bc < 14 && bc < qsize) {
      const idx = (qh + bc) & Q_MASK;
      const clen = q[idx].length | 0;
      if (bb > 0 && (bb + clen) > MERGE_MAX) break;
      bb = (bb + clen) | 0;
      bc = (bc + 1) | 0;
    }
    
    let data;
    if (bc === 1) {
      data = q[qh];
    } else {
      data = new Uint8Array(bb);
      let off = 0;
      for (let i = 0; i < bc; i = (i + 1) | 0) {
        const idx = (qh + i) & Q_MASK;
        const chunk = q[idx];
        data.set(chunk, off);
        off = (off + chunk.length) | 0;
      }
    }
    
    this.qh = (qh + bc) & Q_MASK;
    this.qb = (this.qb - bb) | 0;
    
    try {
      await w.ready;
      if (s.dead) break;
      await w.write(data);
    } catch {
      s.kill();
      break;
    }
  }
  
  this.lock = false;
};

function Downlink(s, ws, r) {
  this.s = s;
  this.ws = ws;
  this.r = r;
  this.first = true;
  this.run();
}

Downlink.prototype.run = async function() {
  const s = this.s;
  const ws = this.ws;
  const r = this.r;
  let first = this.first;
  
  try {
    while (!s.dead) {
      let buf = ws.bufferedAmount | 0;
      
      if (buf > WS_HI) {
        let cnt = 0;
        await new Promise(res => {
          function chk() {
            if (s.dead || ws.bufferedAmount < WS_LO) {
              res();
            } else {
              cnt = (cnt + 1) | 0;
              if (cnt > 15) {
                setTimeout(res, 0);
              } else {
                queueMicrotask(chk);
              }
            }
          }
          chk();
        });
        if (s.dead) break;
      }
      
      buf = ws.bufferedAmount | 0;
      const qt = (buf < WS_LO ? 10 : 3) | 0;
      
      for (let i = 0; i < qt && !s.dead; i = (i + 1) | 0) {
        const {done, value} = await r.read();
        
        if (done) {
          s.kill();
          return;
        }
        
        if (first) {
          const vlen = value.length | 0;
          const frame = new Uint8Array(vlen + 2);
          frame[0] = 0;
          frame[1] = 0;
          frame.set(value, 2);
          ws.send(frame);
          first = false;
          this.first = false;
        } else {
          ws.send(value);
        }
        
        if (ws.bufferedAmount > WS_HI) break;
      }
    }
  } catch {
    s.kill();
  } finally {
    queueMicrotask(() => {
      try { r.releaseLock(); } catch {}
    });
  }
};

export default {
  async fetch(req) {
    if (req.headers.get('Upgrade') !== 'websocket') return R426;
    
    const proto = req.headers.get('Sec-WebSocket-Protocol');
    if (!proto) return R400;
    
    const data = b64dec(proto);
    if (data === B64FAIL) return R400;
    
    const vl = parseVL(data);
    if (!vl.ok) return R403;
    
    let tcp;
    try {
      tcp = await dial(vl.host, vl.port, false);
    } catch {
      try {
        tcp = await dial(vl.host, vl.port, true);
      } catch {
        return R502;
      }
    }
    
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    
    const state = new State(server, tcp);
    const dlen = data.length | 0;
    const doff = vl.off | 0;
    const init = dlen > doff ? data.subarray(doff) : EMPTY;
    const up = new Uplink(state, tcp.writable.getWriter());
    
    if (init.length > 0) up.push(init);
    
    server.addEventListener('message', e => {
      if (e.data instanceof ArrayBuffer) {
        up.push(new Uint8Array(e.data));
      }
    });
    server.addEventListener('close', () => state.kill());
    server.addEventListener('error', () => state.kill());
    
    new Downlink(state, server, tcp.readable.getReader());
    
    return new Response(null, {status: 101, webSocket: client});
  }
};
