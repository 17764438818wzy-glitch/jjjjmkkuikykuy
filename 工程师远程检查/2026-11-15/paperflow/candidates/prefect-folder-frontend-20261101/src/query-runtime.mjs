import { QueryClient, dehydrate } from '@tanstack/query-core';
import { createAsyncStoragePersister } from '@tanstack/query-async-storage-persister';
import { persistQueryClientRestore, persistQueryClientSubscribe } from '@tanstack/query-persist-client-core';
import { createStore, get, set } from 'idb-keyval';

function indexedQueryStorage(legacyStorage) {
  const store = createStore('paperflow-query-cache-v1', 'queries');
  return {
    async getItem(key) {
      const saved = await get(key, store);
      return saved === undefined ? legacyStorage.getItem(key) : saved;
    },
    setItem: (key, value) => set(key, value, store),
  };
}

function createQueryStore(legacyStorage, onStorageError, queryStorage = indexedQueryStorage(legacyStorage)) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, networkMode: 'always', gcTime: Infinity } } });
  const key = 'paperflow-query-cache-v1';
  let refusedRemoval = false;
  let compactProgress = false;

  const removedTasks = new Set();
  function withoutRemovedTasks(serialized) {
    if (!removedTasks.size && !compactProgress) return serialized;
    const saved = JSON.parse(serialized);
    saved.clientState.queries = saved.clientState.queries.filter((query) =>
      !(compactProgress && ['folders', 'classifications'].includes(query.queryKey[2])) &&
      !removedTasks.has(JSON.stringify([query.queryKey[1], query.queryKey[3]]))).map((query) => {
      if (query.queryKey[2] !== 'tasks' || !query.state.data?.tasks) return query;
      return { ...query, state: { ...query.state, data: { ...query.state.data,
        tasks: query.state.data.tasks.filter((task) =>
          !removedTasks.has(JSON.stringify([query.queryKey[1], task.task_id]))) } } };
    });
    return JSON.stringify(saved);
  }
  let writes = Promise.resolve();
  const orderedStorage = {
    getItem: (name) => queryStorage.getItem(name),
    setItem(name, value) {
      writes = writes.catch(() => {}).then(() => queryStorage.setItem(name, withoutRemovedTasks(value)));
      return writes;
    },
  };

  const base = createAsyncStoragePersister({
    storage: orderedStorage,
    key,
    throttleTime: 0,
    retry: () => { onStorageError(); return undefined; },
  });
  const persister = {
    ...base,
    removeClient: async () => { refusedRemoval = true; },
  };
  return {
    async restore() {
      try {
        await persistQueryClientRestore({ queryClient: client, persister, maxAge: Infinity, buster: 'paperflow-v1' });
      } catch (_) {
        refusedRemoval = true;
      }
      if (refusedRemoval) {
        onStorageError();
        return;
      }
      persistQueryClientSubscribe({
        queryClient: client,
        persister,
        buster: 'paperflow-v1',
        dehydrateOptions: {
          shouldDehydrateMutation: () => false,
          shouldDehydrateQuery: (query) => query.queryKey[0] === 'paperflow' && query.state.data !== undefined,
        },
      });
    },

    async removeTask(source, taskId) {
      removedTasks.add(JSON.stringify([source, taskId]));
      const owned = (query) => query.queryKey[0] === 'paperflow' &&
        query.queryKey[1] === source && query.queryKey[3] === taskId;
      const list = (query) => query.queryKey[0] === 'paperflow' &&
        query.queryKey[1] === source && query.queryKey[2] === 'tasks';
      await client.cancelQueries({ predicate: (query) => owned(query) || list(query) });
      client.removeQueries({ predicate: owned });
      client.setQueriesData({ predicate: list }, (value) => value?.tasks
        ? { ...value, tasks: value.tasks.filter((row) => row.task_id !== taskId) } : value);
      const saved = JSON.stringify({ buster: 'paperflow-v1', timestamp: Date.now(),
        clientState: dehydrate(client, {
          shouldDehydrateMutation: () => false,
          shouldDehydrateQuery: (query) => query.queryKey[0] === 'paperflow' && query.state.data !== undefined,
        }) });
      await orderedStorage.setItem(key, saved);
      if (queryStorage !== legacyStorage) legacyStorage.removeItem(key);
    },
    get: (queryKey) => client.getQueryData(queryKey),
    compact() {
      compactProgress = true;
      client.removeQueries({ predicate: (query) => query.queryKey[0] === 'paperflow' && ['folders', 'classifications'].includes(query.queryKey[2]) });
      if (queryStorage !== legacyStorage) {
        try { legacyStorage.removeItem(key); } catch (_) { onStorageError(); }
      }
    },
    fetch: (queryKey, queryFn) => client.fetchQuery({ queryKey, queryFn, staleTime: 0 }),
    cancel: (queryKey) => client.cancelQueries({ queryKey, exact: true }),
  };
}

globalThis.PaperflowQuery = { createQueryStore };
