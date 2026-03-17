import * as dotenv from 'dotenv';
import cors from 'cors';
import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';

dotenv.config({ quiet: true });

const app = express();
app.use(cors());
app.use(express.json());

const SUPABASE_URL = process.env.SUPABASE_URL ?? 'https://zoemonbualktnxhpbebv.supabase.co';
const SUPABASE_PROJECT_REF = process.env.SUPABASE_PROJECT_REF ?? 'zoemonbualktnxhpbebv';
const SUPABASE_PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY ?? 'sb_publishable_Zu2MWJXLGRLh66nmInx3dA_zqeE3nIY';
const SUPABASE_SECRET_KEY =
  process.env.SUPABASE_SECRET_KEY ?? '';
const SUPABASE_REST_BASE_URL = `${SUPABASE_URL.replace(/\/+$/, '')}/rest/v1`;
const SUPABASE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_SECRET_KEY);

const OPS_MODULES = ['inwarding', 'production', 'packing', 'dispatch'] as const;
type WorkerModule = (typeof OPS_MODULES)[number];

interface OperationEvent {
  id: string;
  module: WorkerModule;
  workerId: string;
  workerName: string;
  workerRole: string;
  createdAt: string;
  batchCode: string;
  quantity: number;
  unit: string;
  summary: string;
  payload: Record<string, string | number | boolean | null>;
}

interface WorkerCredential {
  id: string;
  name: string;
  phone: string;
  pin: string;
  role: string;
  allowedModules: WorkerModule[];
  active: boolean;
  createdAt: string;
}

interface EventsApiResponse {
  events: OperationEvent[];
}

interface WorkersApiResponse {
  workers: WorkerCredential[];
}

const MODULE_SET = new Set<string>(OPS_MODULES);
const clients: Response[] = [];
const fallbackEvents: OperationEvent[] = [];

const defaultFallbackWorkers: WorkerCredential[] = [
  {
    id: 'worker-inwarding-1',
    name: 'Inwarding Staff',
    phone: '9876543210',
    pin: '123456',
    role: 'Inwarding_Staff',
    allowedModules: ['inwarding'],
    active: true,
    createdAt: new Date().toISOString(),
  },
  {
    id: 'worker-production-1',
    name: 'Production Operator',
    phone: '9876543211',
    pin: '223344',
    role: 'Production_Operator',
    allowedModules: ['production'],
    active: true,
    createdAt: new Date().toISOString(),
  },
  {
    id: 'worker-packing-1',
    name: 'Packing Staff',
    phone: '9876543212',
    pin: '112233',
    role: 'Packing_Staff',
    allowedModules: ['packing'],
    active: true,
    createdAt: new Date().toISOString(),
  },
  {
    id: 'worker-dispatch-1',
    name: 'Dispatch Staff',
    phone: '9876543213',
    pin: '654321',
    role: 'Dispatch_Staff',
    allowedModules: ['dispatch'],
    active: true,
    createdAt: new Date().toISOString(),
  },
];

const fallbackWorkers = new Map<string, WorkerCredential>(
  defaultFallbackWorkers.map((worker) => [worker.id, worker] as const),
);

function safeText(value: unknown, fallback: string): string {
  if (typeof value !== 'string') {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function safeNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toAllowedModules(modules: unknown): WorkerModule[] {
  if (!Array.isArray(modules)) {
    return [];
  }
  const normalized = modules
    .map((module) => (typeof module === 'string' ? module.trim().toLowerCase() : ''))
    .filter((module): module is WorkerModule => MODULE_SET.has(module));
  return OPS_MODULES.filter((module) => normalized.includes(module));
}

function toPayload(payload: unknown): Record<string, string | number | boolean | null> {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return {};
  }

  const entries = Object.entries(payload as Record<string, unknown>).map(([key, value]) => {
    if (
      typeof value === 'string' ||
      typeof value === 'number' ||
      typeof value === 'boolean' ||
      value === null
    ) {
      return [key, value] as const;
    }
    return [key, String(value)] as const;
  });

  return Object.fromEntries(entries) as Record<string, string | number | boolean | null>;
}

function mapEventRow(row: any): OperationEvent {
  return {
    id: String(row.id),
    module: String(row.module) as WorkerModule,
    workerId: String(row.worker_id),
    workerName: String(row.worker_name),
    workerRole: String(row.worker_role),
    createdAt: String(row.created_at),
    batchCode: String(row.batch_code),
    quantity: safeNumber(row.quantity, 0),
    unit: String(row.unit),
    summary: String(row.summary),
    payload: toPayload(row.payload),
  };
}

function mapWorkerRow(row: any, modulesByWorkerId: Map<string, WorkerModule[]>): WorkerCredential {
  const workerId = String(row.worker_id);
  return {
    id: workerId,
    name: safeText(row.name, 'Unnamed Worker'),
    phone: safeText(row.phone, ''),
    pin: safeText(row.pin, ''),
    role: safeText(row.worker_role, 'Worker'),
    allowedModules: modulesByWorkerId.get(workerId) ?? [],
    active: Boolean(row.active),
    createdAt: safeText(row.created_at, new Date().toISOString()),
  };
}

function broadcastEvent(event: OperationEvent): void {
  clients.forEach((client) => {
    client.write('event: ops-event\n');
    client.write(`data: ${JSON.stringify(event)}\n\n`);
  });
}

async function supabaseRequest<T>(
  path: string,
  options: {
    method?: 'GET' | 'POST' | 'PATCH' | 'DELETE';
    headers?: Record<string, string>;
    body?: unknown;
  } = {},
): Promise<T> {
  if (!SUPABASE_ENABLED) {
    throw new Error('Supabase credentials are not configured.');
  }

  const response = await fetch(`${SUPABASE_REST_BASE_URL}/${path}`, {
    method: options.method ?? 'GET',
    headers: {
      apikey: SUPABASE_SECRET_KEY,
      Authorization: `Bearer ${SUPABASE_SECRET_KEY}`,
      'Content-Type': 'application/json',
      ...(options.headers ?? {}),
    },
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
  });

  const raw = await response.text();
  if (!response.ok) {
    throw new Error(`Supabase request failed (${response.status}): ${raw}`);
  }

  if (!raw) {
    return null as T;
  }

  return JSON.parse(raw) as T;
}

async function fetchEventsFromSupabase(limit: number): Promise<OperationEvent[]> {
  const rows = await supabaseRequest<any[]>(
    `ops_events?select=id,module,worker_id,worker_name,worker_role,created_at,batch_code,quantity,unit,summary,payload&order=created_at.desc&limit=${limit}`,
  );
  return rows.map(mapEventRow);
}

async function fetchWorkersFromSupabase(): Promise<WorkerCredential[]> {
  const workers = await supabaseRequest<any[]>(
    'ops_workers?select=worker_id,name,phone,pin,worker_role,active,created_at&order=created_at.desc&limit=500',
  );
  const accessRows = await supabaseRequest<any[]>(
    'ops_worker_module_access?select=worker_id,module&limit=2000',
  );

  const modulesByWorkerId = new Map<string, WorkerModule[]>();
  accessRows.forEach((row) => {
    const workerId = String(row.worker_id);
    const module = String(row.module).toLowerCase();
    if (!MODULE_SET.has(module)) {
      return;
    }
    const existing = modulesByWorkerId.get(workerId) ?? [];
    if (!existing.includes(module as WorkerModule)) {
      existing.push(module as WorkerModule);
      modulesByWorkerId.set(workerId, existing);
    }
  });

  return workers.map((worker) => mapWorkerRow(worker, modulesByWorkerId));
}

async function fetchWorkerByIdFromSupabase(workerId: string): Promise<WorkerCredential | null> {
  const workers = await supabaseRequest<any[]>(
    `ops_workers?select=worker_id,name,phone,pin,worker_role,active,created_at&worker_id=eq.${encodeURIComponent(workerId)}&limit=1`,
  );
  if (workers.length === 0) {
    return null;
  }

  const accessRows = await supabaseRequest<any[]>(
    `ops_worker_module_access?select=worker_id,module&worker_id=eq.${encodeURIComponent(workerId)}&limit=20`,
  );

  const modulesByWorkerId = new Map<string, WorkerModule[]>();
  accessRows.forEach((row) => {
    const module = String(row.module).toLowerCase();
    if (!MODULE_SET.has(module)) {
      return;
    }
    const existing = modulesByWorkerId.get(workerId) ?? [];
    if (!existing.includes(module as WorkerModule)) {
      existing.push(module as WorkerModule);
      modulesByWorkerId.set(workerId, existing);
    }
  });

  return mapWorkerRow(workers[0], modulesByWorkerId);
}

async function replaceWorkerModulesInSupabase(workerId: string, allowedModules: WorkerModule[]): Promise<void> {
  await supabaseRequest<void>(
    `ops_worker_module_access?worker_id=eq.${encodeURIComponent(workerId)}`,
    {
      method: 'DELETE',
      headers: { Prefer: 'return=minimal' },
    },
  );

  if (allowedModules.length === 0) {
    return;
  }

  await supabaseRequest<void>('ops_worker_module_access?on_conflict=worker_id%2Cmodule', {
    method: 'POST',
    headers: { Prefer: 'resolution=ignore-duplicates,return=minimal' },
    body: allowedModules.map((module) => ({ worker_id: workerId, module })),
  });
}

async function upsertWorkerToSupabase(worker: WorkerCredential): Promise<WorkerCredential> {
  await supabaseRequest<void>('ops_workers?on_conflict=worker_id', {
    method: 'POST',
    headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
    body: {
      worker_id: worker.id,
      name: worker.name,
      phone: worker.phone || null,
      pin: worker.pin || null,
      worker_role: worker.role,
      active: worker.active,
    },
  });

  await replaceWorkerModulesInSupabase(worker.id, worker.allowedModules);
  const stored = await fetchWorkerByIdFromSupabase(worker.id);
  if (!stored) {
    throw new Error('Worker upsert succeeded but worker fetch failed.');
  }
  return stored;
}

async function upsertWorkerFromEventToSupabase(event: OperationEvent): Promise<void> {
  await supabaseRequest<void>('ops_workers?on_conflict=worker_id', {
    method: 'POST',
    headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
    body: {
      worker_id: event.workerId,
      name: event.workerName,
      worker_role: event.workerRole,
      active: true,
    },
  });

  await supabaseRequest<void>('ops_worker_module_access?on_conflict=worker_id%2Cmodule', {
    method: 'POST',
    headers: { Prefer: 'resolution=ignore-duplicates,return=minimal' },
    body: { worker_id: event.workerId, module: event.module },
  });
}

function parseIncomingEvent(body: any): OperationEvent {
  const moduleRaw = safeText(body?.module, '').toLowerCase();
  if (!MODULE_SET.has(moduleRaw)) {
    throw new Error(`Unsupported module: ${body?.module}`);
  }

  return {
    id: safeText(body?.id, uuidv4()),
    module: moduleRaw as WorkerModule,
    workerId: safeText(body?.workerId, 'mobile-worker'),
    workerName: safeText(body?.workerName, 'Mobile Worker'),
    workerRole: safeText(body?.workerRole, 'Worker'),
    createdAt: new Date().toISOString(),
    batchCode: safeText(body?.batchCode, 'N/A'),
    quantity: Math.max(0, safeNumber(body?.quantity, 0)),
    unit: safeText(body?.unit, 'units'),
    summary: safeText(body?.summary, 'Operation event'),
    payload: toPayload(body?.payload),
  };
}

function deriveSourceApp(req: Request): string {
  const header = req.header('X-Client-Platform');
  if (typeof header === 'string' && header.trim().length > 0) {
    return header.trim().toLowerCase();
  }
  return 'unknown';
}

async function createEventInSupabase(event: OperationEvent, sourceApp: string): Promise<OperationEvent> {
  await upsertWorkerFromEventToSupabase(event);

  const rows = await supabaseRequest<any[]>(
    'ops_events?select=id,module,worker_id,worker_name,worker_role,created_at,batch_code,quantity,unit,summary,payload',
    {
      method: 'POST',
      headers: { Prefer: 'return=representation' },
      body: {
        id: event.id,
        module: event.module,
        worker_id: event.workerId,
        worker_name: event.workerName,
        worker_role: event.workerRole,
        created_at: event.createdAt,
        batch_code: event.batchCode,
        quantity: event.quantity,
        unit: event.unit,
        summary: event.summary,
        payload: event.payload,
        source_app: sourceApp,
      },
    },
  );

  if (rows.length === 0) {
    throw new Error('Supabase insert returned no event row.');
  }

  return mapEventRow(rows[0]);
}

function upsertWorkerToFallback(worker: WorkerCredential): WorkerCredential {
  fallbackWorkers.set(worker.id, worker);
  return worker;
}

function upsertWorkerFromEventToFallback(event: OperationEvent): void {
  const existing = fallbackWorkers.get(event.workerId);
  if (!existing) {
    fallbackWorkers.set(event.workerId, {
      id: event.workerId,
      name: event.workerName,
      phone: '',
      pin: '',
      role: event.workerRole,
      allowedModules: [event.module],
      active: true,
      createdAt: event.createdAt,
    });
    return;
  }

  if (!existing.allowedModules.includes(event.module)) {
    existing.allowedModules = [...existing.allowedModules, event.module];
  }
  existing.name = event.workerName;
  existing.role = event.workerRole;
  fallbackWorkers.set(event.workerId, existing);
}

function createEventInFallback(event: OperationEvent): OperationEvent {
  upsertWorkerFromEventToFallback(event);
  fallbackEvents.unshift(event);
  if (fallbackEvents.length > 500) {
    fallbackEvents.pop();
  }
  return event;
}

// --- OPS ENDPOINTS ---

app.get('/api/v1/ops/supabase/config', (_req: Request, res: Response) => {
  res.json({
    projectRef: SUPABASE_PROJECT_REF,
    apiUrl: SUPABASE_URL,
    publishableKey: SUPABASE_PUBLISHABLE_KEY,
    restApiUrl: SUPABASE_REST_BASE_URL,
    dbEnabled: SUPABASE_ENABLED,
    usingFallbackStorage: !SUPABASE_ENABLED,
  });
});

app.post('/api/v1/ops/events', async (req: Request, res: Response) => {
  try {
    const parsed = parseIncomingEvent(req.body);
    const sourceApp = deriveSourceApp(req);

    let createdEvent: OperationEvent;
    try {
      createdEvent = SUPABASE_ENABLED
        ? await createEventInSupabase(parsed, sourceApp)
        : createEventInFallback(parsed);
    } catch (supabaseError) {
      console.error('Supabase insert failed; using fallback event store.', supabaseError);
      createdEvent = createEventInFallback(parsed);
    }

    broadcastEvent(createdEvent);
    res.status(201).json(createdEvent);
  } catch (error) {
    res.status(400).json({
      error: 'invalid_ops_event',
      message: error instanceof Error ? error.message : 'Invalid operation event payload.',
    });
  }
});

app.get('/api/v1/ops/events', async (req: Request, res: Response) => {
  const limit = Math.min(Math.max(parseInt(String(req.query.limit ?? '200'), 10) || 200, 1), 500);

  let events: OperationEvent[];
  try {
    events = SUPABASE_ENABLED ? await fetchEventsFromSupabase(limit) : fallbackEvents.slice(0, limit);
  } catch (supabaseError) {
    console.error('Supabase read failed; serving fallback events.', supabaseError);
    events = fallbackEvents.slice(0, limit);
  }

  const response: EventsApiResponse = { events };
  res.json(response);
});

app.get('/api/v1/ops/workers', async (_req: Request, res: Response) => {
  let workers: WorkerCredential[];
  try {
    workers = SUPABASE_ENABLED ? await fetchWorkersFromSupabase() : Array.from(fallbackWorkers.values());
  } catch (supabaseError) {
    console.error('Supabase worker read failed; serving fallback workers.', supabaseError);
    workers = Array.from(fallbackWorkers.values());
  }

  const response: WorkersApiResponse = { workers };
  res.json(response);
});

app.post('/api/v1/ops/workers', async (req: Request, res: Response) => {
  const now = new Date().toISOString();
  const allowedModules = toAllowedModules(req.body?.allowedModules);
  const worker: WorkerCredential = {
    id: safeText(req.body?.id, `worker-${uuidv4()}`),
    name: safeText(req.body?.name, 'Unnamed Worker'),
    phone: safeText(req.body?.phone, ''),
    pin: safeText(req.body?.pin, ''),
    role: safeText(req.body?.role, 'Worker'),
    allowedModules: allowedModules.length > 0 ? allowedModules : ['inwarding'],
    active: req.body?.active !== false,
    createdAt: safeText(req.body?.createdAt, now),
  };

  try {
    const stored = SUPABASE_ENABLED ? await upsertWorkerToSupabase(worker) : upsertWorkerToFallback(worker);
    res.status(201).json(stored);
  } catch (error) {
    console.error('Worker upsert failed.', error);
    res.status(500).json({
      error: 'worker_upsert_failed',
      message: error instanceof Error ? error.message : 'Unable to create worker.',
    });
  }
});

app.patch('/api/v1/ops/workers/:workerId/access', async (req: Request, res: Response) => {
  const workerId = safeText(req.params.workerId, '');
  if (!workerId) {
    res.status(400).json({ error: 'invalid_worker_id', message: 'workerId path parameter is required.' });
    return;
  }

  const allowedModules = toAllowedModules(req.body?.allowedModules);
  try {
    if (SUPABASE_ENABLED) {
      const existing = await fetchWorkerByIdFromSupabase(workerId);
      if (!existing) {
        res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
        return;
      }

      await replaceWorkerModulesInSupabase(workerId, allowedModules);
      const stored = await fetchWorkerByIdFromSupabase(workerId);
      res.json(stored);
      return;
    }

    const existing = fallbackWorkers.get(workerId);
    if (!existing) {
      res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
      return;
    }

    existing.allowedModules = allowedModules;
    fallbackWorkers.set(workerId, existing);
    res.json(existing);
  } catch (error) {
    console.error('Worker access update failed.', error);
    res.status(500).json({
      error: 'worker_access_update_failed',
      message: error instanceof Error ? error.message : 'Unable to update worker access.',
    });
  }
});

app.patch('/api/v1/ops/workers/:workerId/credentials', async (req: Request, res: Response) => {
  const workerId = safeText(req.params.workerId, '');
  if (!workerId) {
    res.status(400).json({ error: 'invalid_worker_id', message: 'workerId path parameter is required.' });
    return;
  }

  const newPhone: string | undefined =
    typeof req.body?.phone === 'string' && req.body.phone.trim().length > 0 ? req.body.phone.trim() : undefined;
  const newPin: string | undefined =
    typeof req.body?.pin === 'string' && req.body.pin.trim().length > 0 ? req.body.pin.trim() : undefined;

  if (!newPhone && !newPin) {
    res.status(400).json({ error: 'missing_fields', message: 'Provide at least one of phone or pin.' });
    return;
  }

  const patchBody: Record<string, string> = {};
  if (newPhone) patchBody['phone'] = newPhone;
  if (newPin) patchBody['pin'] = newPin;

  try {
    if (SUPABASE_ENABLED) {
      const rows = await supabaseRequest<any[]>(
        `ops_workers?worker_id=eq.${encodeURIComponent(workerId)}&select=worker_id,name,phone,pin,worker_role,active,created_at`,
        {
          method: 'PATCH',
          headers: { Prefer: 'return=representation' },
          body: patchBody,
        },
      );

      if (rows.length === 0) {
        res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
        return;
      }

      const stored = await fetchWorkerByIdFromSupabase(workerId);
      res.json(stored);
      return;
    }

    const existing = fallbackWorkers.get(workerId);
    if (!existing) {
      res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
      return;
    }

    if (newPhone) existing.phone = newPhone;
    if (newPin) existing.pin = newPin;
    fallbackWorkers.set(workerId, existing);
    res.json(existing);
  } catch (error) {
    console.error('Worker credentials update failed.', error);
    res.status(500).json({
      error: 'worker_credentials_update_failed',
      message: error instanceof Error ? error.message : 'Unable to update worker credentials.',
    });
  }
});

app.delete('/api/v1/ops/workers/:workerId', async (req: Request, res: Response) => {
  const workerId = safeText(req.params.workerId, '');
  if (!workerId) {
    res.status(400).json({ error: 'invalid_worker_id', message: 'workerId path parameter is required.' });
    return;
  }

  try {
    if (SUPABASE_ENABLED) {
      const existing = await fetchWorkerByIdFromSupabase(workerId);
      if (!existing) {
        res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
        return;
      }
      await supabaseRequest<any>(`ops_workers?worker_id=eq.${encodeURIComponent(workerId)}`, {
        method: 'DELETE',
        headers: { Prefer: 'return=minimal' },
      });
      res.status(204).send();
      return;
    }

    if (!fallbackWorkers.has(workerId)) {
      res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
      return;
    }

    fallbackWorkers.delete(workerId);
    res.status(204).send();
  } catch (error) {
    console.error('Worker deletion failed.', error);
    if (error instanceof Error && error.message.includes('foreign key constraint')) {
      res.status(409).json({
        error: 'foreign_key_violation',
        message: 'Cannot delete worker because they are linked to existing operation events. Please deactivate instead.',
      });
      return;
    }
    res.status(500).json({
      error: 'worker_deletion_failed',
      message: error instanceof Error ? error.message : 'Unable to delete worker.',
    });
  }
});

app.patch('/api/v1/ops/workers/:workerId/active', async (req: Request, res: Response) => {
  const workerId = safeText(req.params.workerId, '');
  if (!workerId) {
    res.status(400).json({ error: 'invalid_worker_id', message: 'workerId path parameter is required.' });
    return;
  }
  const active = Boolean(req.body?.active);

  try {
    if (SUPABASE_ENABLED) {
      const rows = await supabaseRequest<any[]>(
        `ops_workers?worker_id=eq.${encodeURIComponent(workerId)}&select=worker_id,name,phone,pin,worker_role,active,created_at`,
        {
          method: 'PATCH',
          headers: { Prefer: 'return=representation' },
          body: { active },
        },
      );

      if (rows.length === 0) {
        res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
        return;
      }

      const stored = await fetchWorkerByIdFromSupabase(workerId);
      res.json(stored);
      return;
    }

    const existing = fallbackWorkers.get(workerId);
    if (!existing) {
      res.status(404).json({ error: 'worker_not_found', message: `Worker ${workerId} does not exist.` });
      return;
    }

    existing.active = active;
    fallbackWorkers.set(workerId, existing);
    res.json(existing);
  } catch (error) {
    console.error('Worker active status update failed.', error);
    res.status(500).json({
      error: 'worker_active_update_failed',
      message: error instanceof Error ? error.message : 'Unable to update worker active status.',
    });
  }
});

app.get('/api/v1/ops/events/stream', (_req: Request, res: Response) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  clients.push(res);
  res.write('event: ready\n');
  res.write(`data: ${JSON.stringify({ connected: true })}\n\n`);

  res.on('close', () => {
    const index = clients.indexOf(res);
    if (index !== -1) {
      clients.splice(index, 1);
    }
  });
});

// --- AUTH ENDPOINTS ---

const MOCK_TOKENS = {
  accessToken: 'mock-access-token',
  refreshToken: 'mock-refresh-token',
  expiresIn: 3600,
};

async function fetchWorkerByPhoneFromSupabase(phone: string): Promise<WorkerCredential | null> {
  const workers = await supabaseRequest<any[]>(
    `ops_workers?select=worker_id,name,phone,pin,worker_role,active,created_at&phone=eq.${encodeURIComponent(phone)}&active=eq.true&limit=1`,
  );
  if (workers.length === 0) {
    return null;
  }
  const workerId = String(workers[0].worker_id);
  const accessRows = await supabaseRequest<any[]>(
    `ops_worker_module_access?select=worker_id,module&worker_id=eq.${encodeURIComponent(workerId)}&limit=20`,
  );
  const modulesByWorkerId = new Map<string, WorkerModule[]>();
  accessRows.forEach((row) => {
    const module = String(row.module).toLowerCase();
    if (!MODULE_SET.has(module)) return;
    const existing = modulesByWorkerId.get(workerId) ?? [];
    if (!existing.includes(module as WorkerModule)) {
      existing.push(module as WorkerModule);
      modulesByWorkerId.set(workerId, existing);
    }
  });
  return mapWorkerRow(workers[0], modulesByWorkerId);
}

app.post('/api/v1/auth/login/phone', async (req: Request, res: Response) => {
  const phone = safeText(req.body?.phone, '');
  const pin = safeText(req.body?.pin, '');

  if (!phone) {
    res.status(400).json({ error: 'missing_phone', message: 'Phone number is required.' });
    return;
  }

  if (!pin) {
    res.status(400).json({ error: 'missing_pin', message: 'PIN is required.' });
    return;
  }

  let worker: WorkerCredential | null = null;

  if (SUPABASE_ENABLED) {
    try {
      worker = await fetchWorkerByPhoneFromSupabase(phone);
    } catch (err) {
      console.error('Supabase worker fetch failed during login, falling back to memory store.', err);
    }
  }

  if (!worker) {
    worker = Array.from(fallbackWorkers.values()).find((w) => w.phone === phone && w.active) ?? null;
  }

  if (!worker) {
    res.status(401).json({ error: 'invalid_credentials', message: 'Invalid phone number or PIN.' });
    return;
  }

  if (worker.pin !== pin) {
    res.status(401).json({ error: 'invalid_credentials', message: 'Invalid phone number or PIN.' });
    return;
  }

  res.json({
    ...MOCK_TOKENS,
    user: {
      userId: worker.id,
      tenantId: 'tenant-1',
      phone: worker.phone,
      name: worker.name,
      role: worker.role,
      factoryIds: [],
      allowedModules: worker.allowedModules,
    },
  });
});

app.post('/api/v1/auth/refresh', (_req: Request, res: Response) => {
  res.json({ ...MOCK_TOKENS });
});

app.post('/api/v1/auth/logout', (_req: Request, res: Response) => {
  res.status(200).send();
});

app.get('/api/v1/auth/me', (_req: Request, res: Response) => {
  res.json({ userId: 'admin-1', tenantId: 'tenant-1', name: 'Admin User', role: 'Platform_Admin', factoryIds: ['factory-1'], phone: '9999999999' });
});

app.get('/api/v1/auth/permissions', (_req: Request, res: Response) => {
  res.json([
    { module: 'dashboard', action: 'read', resourceScope: 'tenant' },
    { module: 'inwarding', action: 'create', resourceScope: 'factory' },
    { module: 'production', action: 'create', resourceScope: 'factory' },
    { module: 'packing', action: 'create', resourceScope: 'factory' },
    { module: 'dispatch', action: 'create', resourceScope: 'factory' },
  ]);
});

app.post('/api/v1/auth/sync/events', (req: Request, res: Response) => {
  res.json({ syncedCount: req.body.events?.length || 0, conflicts: [] });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
