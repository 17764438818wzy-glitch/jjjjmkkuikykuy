// Protocol ordering only. Native SDK/model acceptance is recorded separately.
const fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const crypto = require('node:crypto'), assert = require('node:assert/strict');
const { test } = require('node:test');
const source = fs.readFileSync(path.join(__dirname, '../paperflow/native_group.mjs'), 'utf8')
  .replace(/^import .*;\r?$/gm, '').replace(/export /g, '');
const epoch = '2'.repeat(32), base = path.resolve('isolated-group-root');
function fixture() {
  const handlers = new Map(), registry = new Map(), runs = [];
  let requestHandler, sdk;
  const ctx = {
    agents: { get: id => registry.get(String(id)), create: async request => {
      const id = request.sessionId, agent = { id, session: { id, header: request.meta } };
      const done = Promise.withResolvers(); registry.set(id, agent);
      const handle = { agent, id, disposeCalls: 0, hold: false,
        async dispose() { this.disposeCalls++; if (this.hold) await done.promise; registry.delete(id); },
        unblock: () => done.resolve() };
      runs.push(handle); return handle;
    } },
    subagents: { start: async () => {
      const id = crypto.randomUUID(), agent = { id, session: { id } };
      const done = Promise.withResolvers(); registry.set(id, agent);
      const run = { id, localAgent: agent, result: Promise.resolve({ stopReason: 'completed' }),
        disposeCalls: 0, hold: false,
        async dispose() { this.disposeCalls++; if (this.hold) await done.promise; registry.delete(id); },
        unblock: () => done.resolve() };
      runs.push(run); return run;
    } },
    get: () => undefined,
    on(name, fn) { const values = handlers.get(name) || []; values.push(fn); handlers.set(name, values); return () => {}; },
    effect: fn => fn(), root: { fiber: { dispose: async () => {} } },
  };
  class Transport {
    onRequest(fn) { requestHandler = fn; } notify() {} start() {} close() {} async flush() {}
  }
  class Sdk {
    constructor() { sdk = this; this.cwd = base; this.sessions = new Map(); }
    async initialize() { return {}; }
    async getOrCreateSession(id) { const agent = { id, session: { id } }; registry.set(id, agent); return { handle: { agent } }; }
    async prompt({ sessionId }) {
      const agent = registry.get(sessionId);
      for (const fn of handlers.get('session/event') || []) {
        fn(agent.session, { type: 'turn/start', data: {} });
        fn(agent.session, { type: 'turn/end', data: { reason: { kind: 'completed' } } });
      }
      for (const fn of handlers.get('agent/status') || []) fn({agent, status: 'idle'});
    }
    async shutdown() { return {}; }
  }
  const context = {
    resolve: path.resolve, relative: path.relative, isAbsolute: path.isAbsolute, randomUUID: crypto.randomUUID,
    JsonRpcLineTransport: Transport, HarnessSdkJsonRpcServer: Sdk,
    resolveChildAgentOptions: () => ({delegationDepth: 1}), applyChildComposition() {},
    captureDelegatedPolicyOverrides: () => ({}), appendDelegatedPolicyOverrides() {},
    process: { env: { PF_GROUP_ID: '1'.repeat(32), PF_CONTROLLER_EPOCH: epoch }, stdin: {}, stdout: {}, exit() {} },
    setInterval: () => 1, clearInterval() {}, setImmediate: () => 0,
    performance, Promise, Map, Set, Number, Error, AbortController,
  };
  vm.createContext(context); vm.runInContext(source + ';globalThis.load = apply;', context); context.load(ctx);
  return { request: (method, params) => requestHandler(method, params), runs, registry, ctx, get sdk() { return sdk; } };
}
function args(action_id, directory = action_id) { return { epoch, action_id, cwd: path.join(base, directory), prompt: 'fixture', seconds: 30 }; }
test('old action cannot close a newer continuation', async () => {
  const f = fixture(); await f.request('initialize', {});
  const a = 'a'.repeat(32), b = 'b'.repeat(32);
  const first = (await f.request('group/start', args(a))).value;
  await f.request('group/wait', { epoch, action_id: a });
  await f.request('group/continue', { ...args(b), child_id: first.child_id });
  await assert.rejects(f.request('group/close', { epoch, action_id: a }));
  assert.equal(f.runs[0].disposeCalls, 0);
});
test('unconfirmed close retains its slot and refuses continuation', async () => {
  const f = fixture(); await f.request('initialize', {}); const ids = [];
  for (let i = 0; i < 8; i++) {
    const id = i.toString(16).padStart(32, '0'); ids.push(id);
    await f.request('group/start', args(id)); await f.request('group/wait', { epoch, action_id: id });
  }
  f.runs[0].hold = true;
  const closing = f.request('group/close', { epoch, action_id: ids[0] }); await Promise.resolve();
  try {
    await assert.rejects(f.request('group/start', args('f'.repeat(32))));
    await assert.rejects(f.request('group/continue', { ...args('e'.repeat(32)), child_id: f.runs[0].id }));
    assert.equal(f.registry.size - 1, 8);
  } finally { f.runs[0].unblock(); await closing; }
});
test('shutdown cancels and waits for pending native creation', async () => {
  const f = fixture(); await f.request('initialize', {});
  const original = f.ctx.agents.create, gate = Promise.withResolvers(), entered = Promise.withResolvers(); let signal;
  f.ctx.agents.create = async request => { signal = request.signal; entered.resolve(); await gate.promise; return original(request); };
  const start = f.request('group/start', args('a'.repeat(32))); const outcome = start.then(() => 'accepted', () => 'rejected');
  await entered.promise; let shutdownDone = false;
  const shutdown = f.request('shutdown', {}).then(() => { shutdownDone = true; });
  await new Promise(setImmediate);
  try { assert.equal(signal.aborted, true); assert.equal(shutdownDone, false); }
  finally { gate.resolve(); await shutdown; }
  assert.equal(await outcome, 'rejected'); assert.equal(f.registry.size - 1, 0);
});
test('native factory receives the article directory before publication', async () => {
  const f=fixture(); await f.request('initialize', {});
  const reply=(await f.request('group/start', args('a'.repeat(32)))).value;
  assert.equal(f.registry.get(reply.child_id).session.header.cwd, args('a'.repeat(32)).cwd);
});
test('pre-admission rejection leaves no hanging frame or consumed action', async () => {
  const f=fixture(); await f.request('initialize', {}); const id='a'.repeat(32);
  await assert.rejects(f.request('group/start', {...args(id),cwd:path.dirname(base)}));
  await assert.rejects(f.request('group/wait', {epoch,action_id:id}));
  await f.request('group/start', args(id));
  assert.equal((await f.request('group/wait', {epoch,action_id:id})).value.finish_reason,'completed');
});
