/** Thin protocol over the installed Harness Agent/Subagent/SDK services. */
import { resolve, relative, isAbsolute } from 'node:path';
import { randomUUID } from 'node:crypto';
import { JsonRpcLineTransport } from '/opt/dsh/node_modules/@deepseek-ai/dsh-sdk-protocol/lib/index.js';
import { HarnessSdkJsonRpcServer } from '/opt/dsh/node_modules/@deepseek-ai/dsh-sdk-jsonrpc-server/lib/index.js';
import { resolveChildAgentOptions, applyChildComposition, captureDelegatedPolicyOverrides, appendDelegatedPolicyOverrides } from '/opt/dsh/node_modules/@deepseek-ai/dsh-subagent/lib/types/child-agent.js';

export const name = 'paperflow-group-native';
export const inject = ['agents', 'subagents', 'sdkAppStartup', 'loader'];

export function apply(ctx) {
  const group = process.env.PF_GROUP_ID;
  const epoch = process.env.PF_CONTROLLER_EPOCH;
  if (!/^[a-f0-9]{32}$/.test(group ?? '') || !/^[a-f0-9]{32}$/.test(epoch ?? '')) throw new Error('group identity absent');
  const transport = new JsonRpcLineTransport(process.stdin, process.stdout);
  const sdk = new HarnessSdkJsonRpcServer(ctx, transport, { maxTokensAsSuccess: false });
  const frames = new Map(), children = new Map(), pendingCwds = new Map();
  const owned = () => [...new Set([...children.values(), ...pendingCwds.values()])];
  let parent, closing = false;
  const reply = value => ({ value });
  function authorize(params) {
    if (closing || !parent || ctx.agents.get(parent.id) !== parent || params.epoch !== epoch) throw new Error('group admission closed');
    if (!/^[a-f0-9]{32}$/.test(params.action_id ?? '')) throw new Error('action identity invalid');
  }
  function fresh(params) {
    authorize(params);
    if (frames.has(params.action_id)) throw new Error('action already consumed');
    if (frames.size >= 128) throw new Error('group rotation required');
    if (typeof params.prompt !== 'string' || params.prompt.length === 0 || params.prompt.length > 1024 * 1024) throw new Error('prompt size invalid');
    if (!Number.isFinite(params.seconds) || params.seconds <= 0 || params.seconds > 86400) throw new Error('action deadline invalid');
    const done = Promise.withResolvers();
    const frame = { action_id: params.action_id, deadline: performance.now() + params.seconds * 1000, events: [], done, finished: false };
    frames.set(frame.action_id, frame); return frame;
  }
  function finish(frame, reason) {
    if (frame.finished) return;
    frame.finished = true;
    frame.done.resolve({ action_id: frame.action_id, child_id: String(frame.child.id),
      finish_reason: reason === 'completed' ? 'completed' : 'error', events: frame.events });
  }
  const disposers = [];
  disposers.push(ctx.on('session/created', session => {
    const owner = pendingCwds.get(session.header.cwd);
    if (owner && String(session.header.parentSession) === String(parent?.session.id)) {
      owner.id = session.id; children.set(String(session.id), owner);
    }
  }));
  disposers.push(ctx.on('session/event', (session, event) => {
    const child = children.get(String(session.id)), frame = child?.frame;
    if (!frame || frame.finished) return;
    if (event.type === 'assistant/message' && event.data?.usage) frame.events.push({ type: event.type, data: { usage: event.data.usage } });
    if (event.type === 'turn/end') {
      frame.events.push(event); frame.terminal = event.data?.reason?.kind;
    }
    if (event.type === 'turn/start') frame.running = true;
    if (frame.events.length > 1000) frame.events.shift();
  }));
  disposers.push(ctx.on('agent/status', ({ agent, status }) => {
    const child = children.get(String(agent.id)), frame = child?.frame;
    if (frame && !frame.initial && status === 'idle' && frame.running && frame.terminal) finish(frame, frame.terminal);
  }));
  async function closeChild(child) {
    child.closeTask ??= (async () => {
      child.closing = true;
      child.abort?.abort();
      const created = await child.created?.catch(() => undefined);
      child.run ??= created;
      if (child.run) await child.run.dispose();
      if (ctx.agents.get(child.id)) throw new Error('child teardown unconfirmed');
      child.closed = true;
      if (child.frame && !child.frame.finished) finish(child.frame, 'aborted');
      sdk.sessions.delete(String(child.id));
      children.delete(String(child.id));
      child.run = undefined; child.created = undefined; child.abort = undefined;
      return { child_id: String(child.id), child_closed: true };
    })();
    return child.closeTask;
  }
  async function start(params) {
    authorize(params);
    const cwd = resolve(params.cwd ?? '');
    const rel = relative(sdk.cwd, cwd);
    if (!rel || rel.startsWith('..') || isAbsolute(rel)) throw new Error('child directory outside group');
    if (owned().filter(v => !v.closed).length >= 8) throw new Error('group capacity occupied');
    if (pendingCwds.has(cwd) || [...children.values()].some(v => v.cwd === cwd && !v.closed)) throw new Error('child directory already owned');
    const frame = fresh(params);
    const child = { id: randomUUID(), cwd, frame, closed: false, abort: new AbortController() }; frame.child = child; frame.initial = true;
    pendingCwds.set(cwd, child);
    try {
      // The installed spawn provider ignores a caller cwd and inherits its
      // parent's immutable cwd. Reuse the same native Agent factory that the
      // SDK owns, specifying the child's directory before publication.
      child.created = ctx.agents.create({
        sessionId: child.id, parentAgent: parent,
        meta: { cwd, parentSession: parent.session.id, origin: 'subagent', delegationDepth: 1 },
        agentOptions: resolveChildAgentOptions(parent, undefined, 1), signal: child.abort.signal,
        setup(childCtx, agent) {
          applyChildComposition(childCtx, parent, {});
          appendDelegatedPolicyOverrides(agent.session, { ...captureDelegatedPolicyOverrides(parent),
            sandboxMode: 'workspace-write', approvalPolicy: 'never' });
        },
      }).then(handle => ({ id: handle.agent.id, localAgent: handle.agent, dispose: () => handle.dispose() }));
      const created = await child.created;
      if (closing || child.closing || child.closed || performance.now() >= frame.deadline) {
        await closeChild(child); throw new Error('child creation cancelled');
      }
      child.run = created;
      child.id = child.run.id; children.set(String(child.id), child);
      if (child.run.localAgent.session.header.cwd !== cwd) throw new Error('child directory identity mismatch');
      frame.initial = false;
      sdk.sessions.set(String(child.id), { handle: { agent: child.run.localAgent, dispose: () => child.run.dispose() } });
      await sdk.prompt({ sessionId: String(child.id), contentBlocks: [{ type: 'text', text: params.prompt }] });
      if (closing || child.closing || performance.now() >= frame.deadline) throw new Error('child prompt cancelled');
      return reply({ child_id: String(child.id), action_id: frame.action_id });
    } catch (error) {
      await closeChild(child);
      if (!frame.finished) finish(frame, 'error');
      throw error;
    } finally { pendingCwds.delete(cwd); }
  }
  async function continuation(params) {
    authorize(params);
    const child = children.get(params.child_id);
    if (!child || child.closed || child.closing || !child.frame.finished || !child.run?.localAgent || ctx.agents.get(child.id) !== child.run.localAgent) throw new Error('child continuation unavailable');
    const frame = fresh(params); frame.child = child; frame.initial = false; child.frame = frame;
    sdk.sessions.set(String(child.id), { handle: { agent: child.run.localAgent, dispose: () => child.run.dispose() } });
    try {
      await sdk.prompt({ sessionId: String(child.id), contentBlocks: [{ type: 'text', text: params.prompt }] });
      if (closing || child.closing || performance.now() >= frame.deadline) throw new Error('continuation cancelled');
    } catch (error) { await closeChild(child); throw error; }
    return reply({ child_id: String(child.id), action_id: frame.action_id });
  }
  const timer = setInterval(() => {
    for (const child of owned()) {
      if (!child.closed && child.frame && !child.frame.finished && performance.now() >= child.frame.deadline) {
        closeChild(child).catch(() => transport.notify('group.failure', { reason: 'child_exit_unknown' }));
      }
    }
  }, 100);
  transport.onRequest(async (method, params) => {
    if (method === 'initialize') {
      await ctx.get('loader')?.await();
      const value = await sdk.initialize(params);
      parent = (await sdk.getOrCreateSession('group-' + group)).handle.agent;
      return value;
    }
    if (method === 'group/start') return start(params);
    if (method === 'group/continue') return continuation(params);
    if (method === 'group/wait') {
      authorize(params); const frame = frames.get(params.action_id);
      if (!frame) throw new Error('unknown action');
      return reply(await frame.done.promise);
    }
    if (method === 'group/close') {
      authorize(params); const frame = frames.get(params.action_id);
      if (!frame) throw new Error('unknown action');
      if (frame.child.frame !== frame) throw new Error('superseded action cannot close current child');
      const proof = await closeChild(frame.child);
      return reply({ ...proof, others_live: [...children.values()].filter(v => !v.closed).length });
    }
    if (method === 'group/status') {
      authorize(params);
      return reply({ group_id: group, epoch, children_owned: owned().filter(v => !v.closed).length,
        active_children: owned().filter(v => !v.closed && v.frame?.running && !v.frame?.finished).length, actions: frames.size });
    }
    if (method === 'shutdown') {
      closing = true; clearInterval(timer);
      await Promise.all(owned().map(closeChild));
      const value = await sdk.shutdown();
      setImmediate(async () => { await transport.flush(); await ctx.root.fiber.dispose(); process.exit(0); });
      return value;
    }
    throw new Error('unsupported managed group method');
  });
  ctx.effect(() => {
    transport.start();
    return async () => {
      closing = true; clearInterval(timer);
      await Promise.allSettled(owned().map(closeChild));
      await sdk.shutdown(); transport.close();
      for (const dispose of disposers) dispose();
    };
  }, 'paperflow.group.serve');
}
