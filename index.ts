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

/**
 * Generates the daily batch code from a given date.
 *
 * Format: {day-digits-as-letters}{MM}{YY}
 *   Digit map: 0=A 1=B 2=C 3=D 4=E 5=F 6=G 7=H 8=I 9=J
 *
 * Example: 18-03-2026  →  day=18 → "BI",  month=03,  year=26  →  "BI0326"
 *
 * One batch code per calendar day — all modules share it.
 */
const DIGIT_TO_LETTER: Record<string, string> = {
  '0': 'A', '1': 'B', '2': 'C', '3': 'D', '4': 'E',
  '5': 'F', '6': 'G', '7': 'H', '8': 'I', '9': 'J',
};

function generateBatchCode(date: Date = new Date()): string {
  const dd = String(date.getDate()).padStart(2, '0');
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const yy = String(date.getFullYear()).slice(-2);
  const dayLetters = dd.split('').map((d) => DIGIT_TO_LETTER[d]).join('');
  return `${dayLetters}${mm}${yy}`;
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
    batchCode: generateBatchCode(new Date()),
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

// --- PRODUCTION INVENTORY DEDUCTION ---

interface RecipeLineRow {
  ingredient_id: string;
  qty: number;
}

interface IngredientStockRow {
  id: string;
  name: string;
  current_stock: number;
  reorder_point: number;
  default_unit: string;
}

interface InventoryDeduction {
  ingredientId: string;
  ingredientName: string;
  deductedQty: number;
  unit: string;
  previousStock: number;
  newStock: number;
  belowReorderPoint: boolean;
}

interface ProductionDeductionResult {
  recipeFound: boolean;
  skipped: boolean;
  skipReason?: string;
  deductions: InventoryDeduction[];
  lowStockAlerts: string[];
}

/**
 * Returns the recipe_id linked to a SKU, or null if not found.
 */
async function fetchRecipeIdForSku(skuId: string): Promise<string | null> {
  try {
    const rows = await supabaseRequest<any[]>(
      `gg_skus?select=id,recipe_id&id=eq.${encodeURIComponent(skuId)}&limit=1`,
    );
    const recipeId = rows[0]?.recipe_id;
    return typeof recipeId === 'string' && recipeId ? recipeId : null;
  } catch {
    return null;
  }
}

/**
 * Returns the reference batch size for a recipe.
 * Tries `batch_size` column first, falls back to `yield_factor`.
 */
async function fetchRecipeBatchSize(recipeId: string): Promise<number | null> {
  // The web app stores the batch size in `batch_size_kg`. Older code looked
  // for `batch_size` / `yield_factor` which don't exist — that silently
  // disabled production deduction. Read batch_size_kg first.
  const rows = await supabaseRequest<any[]>(
    `gg_recipes?select=id,batch_size_kg,batch_size,yield_factor&id=eq.${encodeURIComponent(recipeId)}&limit=1`,
  );
  if (rows.length === 0) return null;
  const row = rows[0];
  const batchSize = Number(row.batch_size_kg ?? row.batch_size ?? row.yield_factor);
  return Number.isFinite(batchSize) && batchSize > 0 ? batchSize : null;
}

/**
 * Returns all ingredient lines for a recipe from recipe_lines.
 * (The table is `recipe_lines`, not `gg_recipe_lines` — the old name was a
 * bug that made this always return nothing.)
 */
async function fetchRecipeLines(recipeId: string): Promise<RecipeLineRow[]> {
  const rows = await supabaseRequest<any[]>(
    `recipe_lines?select=ingredient_id,qty&recipe_id=eq.${encodeURIComponent(recipeId)}`,
  );
  return rows
    .map((r) => ({ ingredient_id: String(r.ingredient_id), qty: Number(r.qty) }))
    .filter((r) => r.ingredient_id && Number.isFinite(r.qty) && r.qty > 0);
}

/**
 * Fetches current stock rows for multiple ingredients in a single request.
 */
async function fetchIngredientStocks(ingredientIds: string[]): Promise<Map<string, IngredientStockRow>> {
  if (ingredientIds.length === 0) return new Map();
  const inClause = ingredientIds.map(encodeURIComponent).join(',');
  const rows = await supabaseRequest<any[]>(
    `gg_ingredients?select=id,name,current_stock,reorder_point,default_unit&id=in.(${inClause})`,
  );
  const map = new Map<string, IngredientStockRow>();
  for (const row of rows) {
    map.set(String(row.id), {
      id: String(row.id),
      name: String(row.name ?? 'Unknown'),
      current_stock: Number(row.current_stock ?? 0),
      reorder_point: Number(row.reorder_point ?? 0),
      default_unit: String(row.default_unit ?? 'kg'),
    });
  }
  return map;
}

/**
 * PATCHes a single ingredient's current_stock to the new value (floor 0).
 */
async function patchIngredientStock(ingredientId: string, newStock: number): Promise<void> {
  await supabaseRequest<void>(
    `gg_ingredients?id=eq.${encodeURIComponent(ingredientId)}`,
    {
      method: 'PATCH',
      headers: { Prefer: 'return=minimal' },
      body: { current_stock: Math.max(0, newStock) },
    },
  );
}

/**
 * Orchestrates automatic inventory deduction for a production event.
 *
 * Steps:
 *   1. Resolve recipe_id from payload (direct or via sku_id → gg_skus)
 *   2. Fetch recipe batch_size and all recipe lines in parallel
 *   3. Batch-fetch current stock for every ingredient in the recipe
 *   4. Deduct proportionally: deduct = recipe_qty × (actual_output / batch_size)
 *   5. PATCH each ingredient's current_stock; flag any that fall ≤ reorder_point
 *
 * This function never throws — errors are logged so they never block the event save.
 */
async function runProductionInventoryDeduction(event: OperationEvent): Promise<ProductionDeductionResult> {
  const empty: ProductionDeductionResult = {
    recipeFound: false,
    skipped: true,
    deductions: [],
    lowStockAlerts: [],
  };

  const actualOutputQty = event.quantity;
  if (!(actualOutputQty > 0)) {
    return { ...empty, skipReason: 'Output quantity is zero or missing' };
  }

  // 1. Resolve recipe ID
  let recipeId: string | null = null;
  if (typeof event.payload.recipe_id === 'string' && event.payload.recipe_id) {
    recipeId = event.payload.recipe_id;
  } else if (typeof event.payload.sku_id === 'string' && event.payload.sku_id) {
    recipeId = await fetchRecipeIdForSku(event.payload.sku_id);
  }

  if (!recipeId) {
    return { ...empty, skipReason: 'No recipe linked to this production event' };
  }

  // 2. Fetch batch size + recipe lines in parallel
  const [batchSize, lines] = await Promise.all([
    fetchRecipeBatchSize(recipeId),
    fetchRecipeLines(recipeId),
  ]);

  if (!batchSize) {
    return { ...empty, recipeFound: true, skipReason: 'Recipe has no batch_size defined' };
  }
  if (lines.length === 0) {
    return { ...empty, recipeFound: true, skipReason: 'Recipe has no ingredient lines' };
  }

  // 3. Batch-fetch current stock for all ingredients
  const stockMap = await fetchIngredientStocks(lines.map((l) => l.ingredient_id));

  // 4. Calculate and apply deductions
  const ratio = actualOutputQty / batchSize;
  const deductions: InventoryDeduction[] = [];
  const lowStockAlerts: string[] = [];

  for (const line of lines) {
    const stock = stockMap.get(line.ingredient_id);
    if (!stock) {
      console.warn(`[inventory] Ingredient ${line.ingredient_id} not found in gg_ingredients, skipping.`);
      continue;
    }

    const deductQty = Math.round(line.qty * ratio * 1000) / 1000; // 3 decimal places
    const newStock = Math.max(0, stock.current_stock - deductQty);

    try {
      await patchIngredientStock(line.ingredient_id, newStock);
    } catch (patchErr) {
      console.error(`[inventory] Failed to deduct stock for "${stock.name}":`, patchErr);
      continue;
    }

    const belowReorderPoint = stock.reorder_point > 0 && newStock <= stock.reorder_point;
    deductions.push({
      ingredientId: line.ingredient_id,
      ingredientName: stock.name,
      deductedQty: deductQty,
      unit: stock.default_unit,
      previousStock: stock.current_stock,
      newStock,
      belowReorderPoint,
    });
    if (belowReorderPoint) {
      lowStockAlerts.push(stock.name);
    }
  }

  return { recipeFound: true, skipped: false, deductions, lowStockAlerts };
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

app.get('/api/v1/ops/batch-code/today', (_req: Request, res: Response) => {
  const today = new Date();
  const dd = String(today.getDate()).padStart(2, '0');
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const yyyy = today.getFullYear();
  res.json({
    batchCode: generateBatchCode(today),
    date: `${yyyy}-${mm}-${dd}`,
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

    // Automatic inventory deduction for production events
    if (parsed.module === 'production' && SUPABASE_ENABLED) {
      let inventoryResult: ProductionDeductionResult = {
        recipeFound: false,
        skipped: true,
        skipReason: 'Deduction not attempted',
        deductions: [],
        lowStockAlerts: [],
      };
      try {
        inventoryResult = await runProductionInventoryDeduction(createdEvent);
        if (inventoryResult.lowStockAlerts.length > 0) {
          console.warn(
            `[inventory] Low stock after production event ${createdEvent.id}:`,
            inventoryResult.lowStockAlerts.join(', '),
          );
        }
      } catch (deductionError) {
        console.error('[inventory] Deduction failed (non-fatal):', deductionError);
      }
      res.status(201).json({
        ...createdEvent,
        inventoryDeductions: inventoryResult.deductions,
        lowStockAlerts: inventoryResult.lowStockAlerts,
      });
      return;
    }

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

// ─── Worker devices (FCM push tokens) ─────────────────────────────────────────
// One row per (worker × device). Token is the unique key — re-registering the
// same physical device just updates worker_id and last_seen_at. Logout calls
// the DELETE endpoint so a logged-out device stops receiving pushes.

app.post('/api/v1/ops/worker-devices', async (req: Request, res: Response) => {
  const workerId = safeText(req.body?.worker_id, '');
  const fcmToken = safeText(req.body?.fcm_token, '');
  const platform = safeText(req.body?.platform, 'android');
  if (!workerId || !fcmToken) {
    res.status(400).json({ error: 'invalid_payload', message: 'worker_id and fcm_token are required.' });
    return;
  }
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled', message: 'Supabase is not configured.' });
    return;
  }
  try {
    const now = new Date().toISOString();
    const stored = await supabaseRequest<any[]>('worker_devices?on_conflict=fcm_token', {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=representation' },
      body: [{ worker_id: workerId, fcm_token: fcmToken, platform, last_seen_at: now, updated_at: now }],
    });
    res.status(201).json(Array.isArray(stored) ? stored[0] ?? null : stored);
  } catch (error) {
    console.error('Worker device upsert failed.', error);
    res.status(500).json({
      error: 'worker_device_upsert_failed',
      message: error instanceof Error ? error.message : 'Unable to register device token.',
    });
  }
});

app.delete('/api/v1/ops/worker-devices/:token', async (req: Request, res: Response) => {
  const token = safeText(req.params.token, '');
  if (!token) {
    res.status(400).json({ error: 'invalid_token', message: 'token path parameter is required.' });
    return;
  }
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled', message: 'Supabase is not configured.' });
    return;
  }
  try {
    await supabaseRequest<any>(`worker_devices?fcm_token=eq.${encodeURIComponent(token)}`, {
      method: 'DELETE',
      headers: { Prefer: 'return=minimal' },
    });
    res.status(204).send();
  } catch (error) {
    console.error('Worker device deletion failed.', error);
    res.status(500).json({
      error: 'worker_device_delete_failed',
      message: error instanceof Error ? error.message : 'Unable to deregister device token.',
    });
  }
});

// ─── D2C dispatch requests ────────────────────────────────────────────────────
// Workers submit per-channel requests from mobile; admin reviews and approves
// line-by-line on web. FIFO is computed at submit (preview) and recomputed at
// approve time. Stock cannot go negative — lines that would push stock < 0 at
// approve time stay pending until stock is replenished.

type FifoSplit = {
  production_batch_id: string | null;
  batch_code: string;
  batch_number: number | null;
  session_date: string | null;
  boxes: number;
};

/**
 * Net available boxes per (flavor, batch_code/production_batch). Includes OPENING-STOCK
 * rows where production_batch_id is null — those are seeded inventory and need to feed
 * dispatch like any other packing row. Returns batches sorted by session_date ASC
 * (oldest first) so FIFO consumes opening stock and earlier production batches first.
 */
async function getFlavorBatchAvailability(flavorId: string): Promise<Array<{
  production_batch_id: string | null;
  batch_code: string;
  batch_number: number | null;
  session_date: string;
  available: number;
}>> {
  // packed per (production_batch_id OR batch_code if null), earliest session_date wins for FIFO
  const packed = await supabaseRequest<any[]>(
    `packing_sessions?select=production_batch_id,batch_code,session_date,boxes_packed&flavor_id=eq.${flavorId}&order=session_date.asc`,
  );
  // dispatched per batch_code (dispatch_events uses batch_code not production_batch_id)
  const dispatched = await supabaseRequest<any[]>(
    `dispatch_events?select=batch_code,boxes_dispatched&sku_id=eq.${flavorId}`,
  );
  // Get batch_number for production_batch_ids we have (skip nulls — OPENING-STOCK
  // has no production_batches row).
  const batchIds = Array.from(new Set(
    packed.filter((p) => p.production_batch_id != null).map((p) => String(p.production_batch_id))
  ));
  const batchInfo = batchIds.length === 0
    ? []
    : await supabaseRequest<any[]>(
        `production_batches?select=id,batch_number&id=in.(${batchIds.join(',')})`,
      );
  const batchNumberById = new Map<string, number | null>();
  batchInfo.forEach((b: any) => batchNumberById.set(String(b.id), b.batch_number ?? null));

  // Aggregate packed by (production_batch_id OR batch_code-when-null); keep earliest session_date.
  const packedByBatch = new Map<string, { production_batch_id: string | null; batch_code: string; session_date: string; packed: number }>();
  for (const row of packed) {
    const pbId = row.production_batch_id != null ? String(row.production_batch_id) : null;
    const key = pbId ?? `code:${String(row.batch_code ?? '')}`;
    const cur = packedByBatch.get(key);
    const boxes = Number(row.boxes_packed) || 0;
    const sessionDate = String(row.session_date ?? '');
    if (cur) {
      cur.packed += boxes;
      if (sessionDate && (!cur.session_date || sessionDate < cur.session_date)) {
        cur.session_date = sessionDate;
      }
    } else {
      packedByBatch.set(key, {
        production_batch_id: pbId,
        batch_code: String(row.batch_code ?? ''),
        session_date: sessionDate,
        packed: boxes,
      });
    }
  }

  const dispatchedByBatchCode = new Map<string, number>();
  for (const row of dispatched) {
    const code = String(row.batch_code ?? '');
    dispatchedByBatchCode.set(code, (dispatchedByBatchCode.get(code) ?? 0) + (Number(row.boxes_dispatched) || 0));
  }

  // Reduce: per batch, available = packed - dispatched_for_that_batch_code
  // (dispatch_events doesn't track production_batch_id, so all dispatches against
  // a batch_code are spread across the production_batches under that code.
  // OPENING-STOCK rows have null production_batch_id and a synthetic key.)
  const result: Array<{
    production_batch_id: string | null;
    batch_code: string;
    batch_number: number | null;
    session_date: string;
    available: number;
  }> = [];

  // Group by batch_code so dispatched is subtracted from packed at the code level
  const codeGroups = new Map<string, Array<{ production_batch_id: string | null; session_date: string; packed: number }>>();
  packedByBatch.forEach((v) => {
    const arr = codeGroups.get(v.batch_code) ?? [];
    arr.push({ production_batch_id: v.production_batch_id, session_date: v.session_date, packed: v.packed });
    codeGroups.set(v.batch_code, arr);
  });

  codeGroups.forEach((rows, code) => {
    rows.sort((a, b) => a.session_date.localeCompare(b.session_date));
    let remainingDispatched = dispatchedByBatchCode.get(code) ?? 0;
    for (const r of rows) {
      const consumed = Math.min(remainingDispatched, r.packed);
      const available = r.packed - consumed;
      remainingDispatched -= consumed;
      if (available > 0) {
        result.push({
          production_batch_id: r.production_batch_id,
          batch_code: code,
          batch_number: r.production_batch_id ? (batchNumberById.get(r.production_batch_id) ?? null) : null,
          session_date: r.session_date,
          available,
        });
      }
    }
  });

  // Final FIFO sort across all batches by session_date ASC.
  result.sort((a, b) => a.session_date.localeCompare(b.session_date));
  return result;
}

/** Compute FIFO split for a single flavor + box count. Returns splits + shortfall. */
async function computeFifoForFlavor(
  flavorId: string,
  boxesRequested: number,
): Promise<{ splits: FifoSplit[]; shortfall: number; totalAvailable: number }> {
  const batches = await getFlavorBatchAvailability(flavorId);
  const totalAvailable = batches.reduce((s, b) => s + b.available, 0);
  const splits: FifoSplit[] = [];
  let remaining = boxesRequested;
  for (const b of batches) {
    if (remaining <= 0) break;
    const take = Math.min(b.available, remaining);
    if (take > 0) {
      splits.push({
        production_batch_id: b.production_batch_id,
        batch_code: b.batch_code,
        batch_number: b.batch_number,
        session_date: b.session_date || null,
        boxes: take,
      });
      remaining -= take;
    }
  }
  return { splits, shortfall: Math.max(0, remaining), totalAvailable };
}

/** Per-flavour finished-goods available — used by mobile picker + web pending tab. */
app.get('/api/v1/ops/finished-goods-available', async (_req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled' });
    return;
  }
  try {
    const flavors = await supabaseRequest<any[]>('gg_flavors?select=id,name&active=eq.true&order=name.asc');
    const packed = await supabaseRequest<any[]>('packing_sessions?select=flavor_id,boxes_packed&limit=100000');
    const dispatched = await supabaseRequest<any[]>('dispatch_events?select=sku_id,boxes_dispatched&limit=100000');

    const packedBy = new Map<string, number>();
    packed.forEach((r: any) => {
      const k = String(r.flavor_id);
      packedBy.set(k, (packedBy.get(k) ?? 0) + (Number(r.boxes_packed) || 0));
    });
    const dispatchedBy = new Map<string, number>();
    dispatched.forEach((r: any) => {
      const k = String(r.sku_id);
      dispatchedBy.set(k, (dispatchedBy.get(k) ?? 0) + (Number(r.boxes_dispatched) || 0));
    });

    const rows = flavors.map((f: any) => ({
      flavor_id: f.id,
      flavor_name: f.name,
      boxes_available: (packedBy.get(f.id) ?? 0) - (dispatchedBy.get(f.id) ?? 0),
    }));
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: 'finished_goods_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Distinct channel names — existing allocations + this table. */
app.get('/api/v1/ops/d2c-channels', async (_req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled' });
    return;
  }
  try {
    const allocations = await supabaseRequest<any[]>('gg_d2c_allocations?select=channel_name&limit=10000');
    const requests = await supabaseRequest<any[]>('d2c_dispatch_requests?select=channel&limit=10000');
    const set = new Set<string>();
    allocations.forEach((r: any) => { if (r.channel_name) set.add(String(r.channel_name)); });
    requests.forEach((r: any) => { if (r.channel) set.add(String(r.channel)); });
    // Seed common channels even if nothing's been used yet
    ['Shopify', 'Amazon', 'Swiggy', 'Zepto', 'Blinkit'].forEach((c) => set.add(c));
    res.json(Array.from(set).sort());
  } catch (error) {
    res.status(500).json({ error: 'channels_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Create a new D2C dispatch request. Validates stock at submit; computes FIFO preview. */
app.post('/api/v1/ops/d2c-requests', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled' });
    return;
  }
  const channel = safeText(req.body?.channel, '').trim();
  const workerId = safeText(req.body?.worker_id, '').trim();
  const notes = safeText(req.body?.notes, '');
  const items: Array<{ flavor_id: string; boxes: number }> = Array.isArray(req.body?.items) ? req.body.items : [];

  if (!channel || !workerId || items.length === 0) {
    res.status(400).json({ error: 'invalid_payload', message: 'channel, worker_id, and items[] are required.' });
    return;
  }

  // Validate stock + compute FIFO per item
  try {
    const itemDetails: Array<{ flavor_id: string; boxes: number; splits: FifoSplit[] }> = [];
    const insufficient: Array<{ flavor_id: string; requested: number; available: number }> = [];
    for (const it of items) {
      const boxes = Math.floor(Number(it.boxes) || 0);
      if (!it.flavor_id || boxes <= 0) {
        res.status(400).json({ error: 'invalid_item', message: 'Each item needs flavor_id and boxes > 0.' });
        return;
      }
      const { splits, shortfall, totalAvailable } = await computeFifoForFlavor(String(it.flavor_id), boxes);
      if (shortfall > 0) {
        insufficient.push({ flavor_id: it.flavor_id, requested: boxes, available: totalAvailable });
        continue;
      }
      itemDetails.push({ flavor_id: it.flavor_id, boxes, splits });
    }
    if (insufficient.length > 0) {
      res.status(409).json({ error: 'insufficient_stock', insufficient });
      return;
    }

    // Insert header
    const headerRows = await supabaseRequest<any[]>('d2c_dispatch_requests', {
      method: 'POST',
      headers: { Prefer: 'return=representation' },
      body: [{ channel, worker_id: workerId, notes, header_status: 'pending' }],
    });
    const requestId = headerRows?.[0]?.id;
    if (!requestId) throw new Error('Header insert returned no id');

    // Insert items
    const itemRows = itemDetails.map((d) => ({
      request_id: requestId,
      flavor_id: d.flavor_id,
      boxes_requested: d.boxes,
      status: 'pending',
      batch_breakdown: d.splits,
    }));
    await supabaseRequest<any[]>('d2c_dispatch_request_items', {
      method: 'POST',
      headers: { Prefer: 'return=minimal' },
      body: itemRows,
    });

    res.status(201).json({ id: requestId, channel, worker_id: workerId, items: itemDetails });
  } catch (error) {
    console.error('D2C request create failed.', error);
    res.status(500).json({ error: 'd2c_request_create_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** List D2C requests, filterable by worker_id, channel, status. */
app.get('/api/v1/ops/d2c-requests', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) {
    res.status(503).json({ error: 'supabase_disabled' });
    return;
  }
  try {
    const conds: string[] = [];
    if (req.query.worker_id) conds.push(`worker_id=eq.${encodeURIComponent(String(req.query.worker_id))}`);
    if (req.query.channel)   conds.push(`channel=eq.${encodeURIComponent(String(req.query.channel))}`);
    if (req.query.status)    conds.push(`header_status=eq.${encodeURIComponent(String(req.query.status))}`);
    const qs = conds.length > 0 ? '&' + conds.join('&') : '';
    const rows = await supabaseRequest<any[]>(
      `d2c_dispatch_requests?select=id,channel,worker_id,header_status,notes,created_at,updated_at,items:d2c_dispatch_request_items(id,flavor_id,boxes_requested,status,batch_breakdown,approved_by,decided_at)&order=created_at.desc${qs}`,
    );
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: 'd2c_request_list_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Single request detail. */
app.get('/api/v1/ops/d2c-requests/:id', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) { res.status(503).json({ error: 'supabase_disabled' }); return; }
  try {
    const rows = await supabaseRequest<any[]>(
      `d2c_dispatch_requests?id=eq.${encodeURIComponent(req.params.id)}&select=id,channel,worker_id,header_status,notes,created_at,updated_at,items:d2c_dispatch_request_items(id,flavor_id,boxes_requested,status,batch_breakdown,approved_by,decided_at,allocation_id)&limit=1`,
    );
    if (!rows || rows.length === 0) {
      res.status(404).json({ error: 'request_not_found' });
      return;
    }
    res.json(rows[0]);
  } catch (error) {
    res.status(500).json({ error: 'd2c_request_detail_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Worker edit: replace channel + replace items (only pending lines actually mutated). */
app.patch('/api/v1/ops/d2c-requests/:id', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) { res.status(503).json({ error: 'supabase_disabled' }); return; }
  const requestId = req.params.id;
  const channel = safeText(req.body?.channel, '').trim();
  const items: Array<{ flavor_id: string; boxes: number }> = Array.isArray(req.body?.items) ? req.body.items : [];
  if (!channel || items.length === 0) {
    res.status(400).json({ error: 'invalid_payload' });
    return;
  }
  try {
    // Validate every line has stock available at edit time
    const itemDetails: Array<{ flavor_id: string; boxes: number; splits: FifoSplit[] }> = [];
    for (const it of items) {
      const boxes = Math.floor(Number(it.boxes) || 0);
      if (!it.flavor_id || boxes <= 0) {
        res.status(400).json({ error: 'invalid_item' });
        return;
      }
      const { splits, shortfall, totalAvailable } = await computeFifoForFlavor(String(it.flavor_id), boxes);
      if (shortfall > 0) {
        res.status(409).json({ error: 'insufficient_stock', flavor_id: it.flavor_id, requested: boxes, available: totalAvailable });
        return;
      }
      itemDetails.push({ flavor_id: it.flavor_id, boxes, splits });
    }

    // Update header channel
    await supabaseRequest<any[]>(`d2c_dispatch_requests?id=eq.${encodeURIComponent(requestId)}`, {
      method: 'PATCH',
      headers: { Prefer: 'return=minimal' },
      body: { channel, updated_at: new Date().toISOString() },
    });

    // Delete existing pending items (already-decided lines stay).
    await supabaseRequest<any[]>(
      `d2c_dispatch_request_items?request_id=eq.${encodeURIComponent(requestId)}&status=eq.pending`,
      { method: 'DELETE', headers: { Prefer: 'return=minimal' } },
    );

    // Insert fresh pending items
    const itemRows = itemDetails.map((d) => ({
      request_id: requestId,
      flavor_id: d.flavor_id,
      boxes_requested: d.boxes,
      status: 'pending',
      batch_breakdown: d.splits,
    }));
    await supabaseRequest<any[]>('d2c_dispatch_request_items', {
      method: 'POST',
      headers: { Prefer: 'return=minimal' },
      body: itemRows,
    });

    res.json({ id: requestId, updated: true });
  } catch (error) {
    res.status(500).json({ error: 'd2c_request_edit_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Worker cancel — sets all pending items to cancelled. Already-decided lines stay. */
app.delete('/api/v1/ops/d2c-requests/:id', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) { res.status(503).json({ error: 'supabase_disabled' }); return; }
  try {
    await supabaseRequest<any[]>(
      `d2c_dispatch_request_items?request_id=eq.${encodeURIComponent(req.params.id)}&status=eq.pending`,
      {
        method: 'PATCH',
        headers: { Prefer: 'return=minimal' },
        body: { status: 'cancelled', decided_at: new Date().toISOString() },
      },
    );
    res.status(204).send();
  } catch (error) {
    res.status(500).json({ error: 'd2c_request_cancel_failed', message: error instanceof Error ? error.message : String(error) });
  }
});

/** Admin per-line decide. Body: { decisions: [{item_id, action: 'approve'|'reject'}], approved_by: '<email>' } */
app.post('/api/v1/ops/d2c-requests/:id/decide', async (req: Request, res: Response) => {
  if (!SUPABASE_ENABLED) { res.status(503).json({ error: 'supabase_disabled' }); return; }
  const decisions: Array<{ item_id: string; action: 'approve' | 'reject' }> = Array.isArray(req.body?.decisions) ? req.body.decisions : [];
  const approvedBy = safeText(req.body?.approved_by, 'web_admin').trim();
  if (decisions.length === 0) {
    res.status(400).json({ error: 'no_decisions' });
    return;
  }

  const requestId = req.params.id;
  // Read the request header to get channel + worker_id (for allocation rows)
  const headerRows = await supabaseRequest<any[]>(
    `d2c_dispatch_requests?id=eq.${encodeURIComponent(requestId)}&select=channel,worker_id&limit=1`,
  );
  if (!headerRows || headerRows.length === 0) {
    res.status(404).json({ error: 'request_not_found' });
    return;
  }
  const channel  = String(headerRows[0].channel);
  const workerId = String(headerRows[0].worker_id);

  const results: Array<{ item_id: string; action: string; status: 'ok' | 'insufficient'; detail?: any }> = [];

  for (const d of decisions) {
    try {
      // Pull the item
      const itemRows = await supabaseRequest<any[]>(
        `d2c_dispatch_request_items?id=eq.${encodeURIComponent(d.item_id)}&select=id,flavor_id,boxes_requested,status&limit=1`,
      );
      const item = itemRows?.[0];
      if (!item) { results.push({ item_id: d.item_id, action: d.action, status: 'ok', detail: 'not_found' }); continue; }
      if (item.status !== 'pending') {
        results.push({ item_id: d.item_id, action: d.action, status: 'ok', detail: `already_${item.status}` });
        continue;
      }

      if (d.action === 'reject') {
        await supabaseRequest<any[]>(
          `d2c_dispatch_request_items?id=eq.${encodeURIComponent(item.id)}`,
          {
            method: 'PATCH',
            headers: { Prefer: 'return=minimal' },
            body: { status: 'rejected', approved_by: approvedBy, decided_at: new Date().toISOString() },
          },
        );
        results.push({ item_id: d.item_id, action: d.action, status: 'ok' });
        continue;
      }

      // approve: recompute FIFO + check stock
      const fifo = await computeFifoForFlavor(String(item.flavor_id), Number(item.boxes_requested));
      if (fifo.shortfall > 0) {
        results.push({
          item_id: d.item_id, action: d.action, status: 'insufficient',
          detail: { requested: item.boxes_requested, available: fifo.totalAvailable },
        });
        continue;
      }

      // Look up flavor_name for the allocation row (existing schema requires it)
      const flavorRows = await supabaseRequest<any[]>(
        `gg_flavors?select=name&id=eq.${encodeURIComponent(item.flavor_id)}&limit=1`,
      );
      const flavorName = String(flavorRows?.[0]?.name ?? '');

      // Insert gg_d2c_allocations row (existing schema + new traceability cols)
      const allocRows = await supabaseRequest<any[]>('gg_d2c_allocations', {
        method: 'POST',
        headers: { Prefer: 'return=representation' },
        body: [{
          channel_name: channel,
          flavor_id: item.flavor_id,
          flavor_name: flavorName,
          boxes_allocated: item.boxes_requested,
          source_request_item_id: item.id,
          requested_by_worker_id: workerId,
        }],
      });
      const allocationId = allocRows?.[0]?.id;

      // Insert dispatch_events per FIFO split (one row per batch)
      const dispatchRows = fifo.splits.map((s) => ({
        batch_code: s.batch_code,
        sku_id: item.flavor_id,
        boxes_dispatched: s.boxes,
        dispatch_date: new Date().toISOString().slice(0, 10),
        customer_name: `D2C — ${channel}`,
      }));
      if (dispatchRows.length > 0) {
        await supabaseRequest<any[]>('dispatch_events', {
          method: 'POST',
          headers: { Prefer: 'return=minimal' },
          body: dispatchRows,
        });
      }

      // Flip item to approved
      await supabaseRequest<any[]>(
        `d2c_dispatch_request_items?id=eq.${encodeURIComponent(item.id)}`,
        {
          method: 'PATCH',
          headers: { Prefer: 'return=minimal' },
          body: {
            status: 'approved',
            approved_by: approvedBy,
            decided_at: new Date().toISOString(),
            allocation_id: allocationId,
          },
        },
      );
      results.push({ item_id: d.item_id, action: d.action, status: 'ok' });
    } catch (error) {
      console.error('Decide line failed.', error);
      results.push({ item_id: d.item_id, action: d.action, status: 'ok', detail: { error: error instanceof Error ? error.message : String(error) } });
    }
  }

  res.json({ request_id: requestId, results });
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

// --- ZOHO INTEGRATION REMOVED ---
//
// The Zoho OAuth flow, the 5-minute polling sync, and the /sync/zoho endpoint
// were removed in favour of a CSV import flow on the dashboard
// (see utpad-web/src/app/features/dashboard/invoices/csv-import.service.ts).
//
// The schema columns gg_invoices.zoho_invoice_id, gg_customers.zoho_customer_id,
// and gg_flavors.zoho_product_id are intentionally retained — the CSV importer
// uses them as stable identifiers across re-imports.

// Export the app for Vercel serverless runtime.
// app.listen() is only called in local development (non-Vercel environments).
export default app;

if (!process.env.VERCEL) {
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}
