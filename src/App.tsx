import { useState, useEffect, useRef, useMemo, FormEvent } from "react";
import { motion, AnimatePresence } from "motion/react";
import {
  Layers,
  Cpu,
  Coins,
  ShieldAlert,
  CheckCircle2,
  XCircle,
  Database,
  BookOpen,
  Terminal,
  FileCode,
  Sliders,
  RefreshCw,
  TrendingUp,
  UserPlus,
  ArrowRightLeft,
  Power,
  Settings,
  Activity,
  HardDrive,
  Lock,
  Unlock,
  CornerDownRight,
  ChevronRight,
  FileText,
  AlertCircle
} from "lucide-react";
import { rustWorkspaceFiles, dbSchemaTables, RustFile, DBTable } from "./data";

// =========================================================================
// REACT SANDBOX TYPE DECLARATIONS
// =========================================================================

enum AppTab {
  Simulator = "simulator",
  Workspace = "workspace",
  Database = "database",
  Manual = "manual",
}

interface PlayerSim {
  uuid: string;
  username: string;
  status: "ACTIVE" | "LOCKED" | "TRANSITIONING" | "BANNED";
  balanceMinor: number;
}

interface EnergyNodeSim {
  id: string;
  displayName: string;
  type: "PRODUCER" | "CONSUMER" | "STORAGE";
  enabled: boolean;
  capacityWatts: number;
  productionWatts: number;
  consumptionWatts: number;
  storedWh: number;
  maxStoredWh: number;
  efficiency: number;
  health: number;
}

interface EventLogSim {
  id: string;
  timestamp: string;
  type: string;
  payload: string;
  correlationId: string;
  isSystem: boolean;
}

export default function App() {
  const [activeTab, setActiveTab] = useState<AppTab>(AppTab.Simulator);
  const [selectedFile, setSelectedFile] = useState<RustFile>(rustWorkspaceFiles[1]); // default to domain/lib.rs
  const [selectedTable, setSelectedTable] = useState<DBTable>(dbSchemaTables[1]); // default to wallets

  // --- Real-time Clock ---
  const [systemTime, setSystemTime] = useState<string>("2026-07-13 13:52:02");
  useEffect(() => {
    const timer = setInterval(() => {
      const d = new Date();
      // Keep mock year 2026 for alignment with context
      const timeStr = `2026-07-13 ${d.toTimeString().split(" ")[0]}`;
      setSystemTime(timeStr);
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  // =========================================================================
  // SIMULATOR STATE
  // =========================================================================

  const [players, setPlayers] = useState<PlayerSim[]>([
    {
      uuid: "7c9ef9d9-c07a-40fc-803a-c3227a696701",
      username: "artemneshaev",
      status: "ACTIVE",
      balanceMinor: 128500, // 1,285.00 ASH
    },
    {
      uuid: "e3d2c88f-fa56-4299-8d19-b53027b68638",
      username: "ashland_admin",
      status: "ACTIVE",
      balanceMinor: 5000000, // 50,000.00 ASH (System Mint)
    },
    {
      uuid: "3aa96bcf-1294-4d8b-968b-7013890f5451",
      username: "industrial_sink",
      status: "ACTIVE",
      balanceMinor: 0,
    },
  ]);

  const [energyNodes, setEnergyNodes] = useState<EnergyNodeSim[]>([
    {
      id: "b0a701d6-4444-4000-8000-000000000001",
      displayName: "Geothermal Steam Core",
      type: "PRODUCER",
      enabled: true,
      capacityWatts: 500000,
      productionWatts: 380000,
      consumptionWatts: 0,
      storedWh: 0,
      maxStoredWh: 0,
      efficiency: 0.95,
      health: 1.0,
    },
    {
      id: "b0a701d6-4444-4000-8000-000000000002",
      displayName: "Create Mod Factory District",
      type: "CONSUMER",
      enabled: true,
      capacityWatts: 400000,
      productionWatts: 0,
      consumptionWatts: 320000,
      storedWh: 0,
      maxStoredWh: 0,
      efficiency: 1.0,
      health: 0.98,
    },
    {
      id: "b0a701d6-4444-4000-8000-000000000003",
      displayName: "Tesla Grid Reserve Battery",
      type: "STORAGE",
      enabled: true,
      capacityWatts: 250000,
      productionWatts: 0,
      consumptionWatts: 0,
      storedWh: 310000,
      maxStoredWh: 1000000,
      efficiency: 0.9,
      health: 1.0,
    },
  ]);

  const [globalReserveWh, setGlobalReserveWh] = useState<number>(310000);
  const [simulationTick, setSimulationTick] = useState<number>(104);
  const [unmetDemandWatts, setUnmetDemandWatts] = useState<number>(0);
  const [energyMode, setEnergyMode] = useState<"NORMAL" | "SURPLUS" | "DEFICIT" | "CRITICAL" | "COLLAPSE">("NORMAL");

  const [logs, setLogs] = useState<EventLogSim[]>([
    {
      id: "ev-1",
      timestamp: "13:50:02",
      type: "PlayerRegistered",
      payload: '{"player_id":"7c9ef9d9-c07a-40fc-803a-c3227a696701","username":"artemneshaev"}',
      correlationId: "ca-9817e812-70b1",
      isSystem: false,
    },
    {
      id: "ev-2",
      timestamp: "13:51:14",
      type: "EnergyNodeRegistered",
      payload: '{"node_id":"b0a701d6-4444-4000-8000-000000000001","region":"industrial_core"}',
      correlationId: "ca-3301a612-4412",
      isSystem: true,
    },
  ]);

  // --- Transactions form state ---
  const [txFrom, setTxFrom] = useState<string>("e3d2c88f-fa56-4299-8d19-b53027b68638");
  const [txTo, setTxTo] = useState<string>("7c9ef9d9-c07a-40fc-803a-c3227a696701");
  const [txAmount, setTxAmount] = useState<number>(150); // in whole ASH credits
  const [simulateIdempotencyDuplicate, setSimulateIdempotencyDuplicate] = useState<boolean>(false);
  const [idempotencyKey, setIdempotencyKey] = useState<string>("88e99bc1-f001-44ab-991c-104992bb3f0a");
  const [processedRequestIds, setProcessedRequestIds] = useState<Set<string>>(new Set());
  const [cachedTransactions, setCachedTransactions] = useState<Record<string, string>>({});

  // --- New player form ---
  const [newUsername, setNewUsername] = useState<string>("");

  // --- Background simulation loop (4 seconds) ---
  const [isSimRunning, setIsSimRunning] = useState<boolean>(true);
  useEffect(() => {
    if (!isSimRunning) return;
    const interval = setInterval(() => {
      triggerSimTick();
    }, 4000);
    return () => clearInterval(interval);
  }, [isSimRunning, energyNodes, globalReserveWh, simulationTick]);

  const addLog = (type: string, payload: string, correlationId: string, isSystem = false) => {
    const time = new Date().toTimeString().split(" ")[0];
    const newLog: EventLogSim = {
      id: `ev-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      timestamp: time,
      type,
      payload,
      correlationId,
      isSystem,
    };
    setLogs((prev) => [newLog, ...prev.slice(0, 49)]); // Keep last 50 logs
  };

  const triggerSimTick = () => {
    setSimulationTick((t) => t + 1);

    let totalProd = 0;
    let totalCons = 0;

    energyNodes.forEach((node) => {
      if (!node.enabled) return;
      if (node.type === "PRODUCER") {
        totalProd += Math.round(node.capacityWatts * node.efficiency * node.health);
      } else if (node.type === "CONSUMER") {
        totalCons += node.capacityWatts;
      } else if (node.type === "STORAGE") {
        // Charging STORAGE acts as consumption
        if (node.productionWatts > 0) {
          totalProd += node.productionWatts;
        } else {
          totalCons += node.consumptionWatts;
        }
      }
    });

    // Tick duration represents 5 seconds
    const tickDurationHours = 5.0 / 3600.0;
    let netWatts = totalProd - totalCons;

    let nextReserve = globalReserveWh;
    let unmetWatts = 0;

    if (netWatts >= 0) {
      const addedWh = Math.round(netWatts * tickDurationHours);
      nextReserve = Math.min(1000000, globalReserveWh + addedWh);
    } else {
      const neededWh = Math.round(Math.abs(netWatts) * tickDurationHours);
      if (nextReserve >= neededWh) {
        nextReserve -= neededWh;
      } else {
        const remainingWh = neededWh - nextReserve;
        nextReserve = 0;
        unmetWatts = Math.round(remainingWh / tickDurationHours);
      }
    }

    setGlobalReserveWh(nextReserve);
    setUnmetDemandWatts(unmetWatts);

    // Sync storage node state with global reserve
    setEnergyNodes((prev) =>
      prev.map((n) => {
        if (n.type === "STORAGE") {
          return { ...n, storedWh: nextReserve };
        }
        return n;
      })
    );

    // Compute mode
    const reserveRatio = nextReserve / 1000000;
    let computedMode: "NORMAL" | "SURPLUS" | "DEFICIT" | "CRITICAL" | "COLLAPSE" = "NORMAL";

    if (unmetWatts > 0) {
      computedMode = "COLLAPSE";
    } else if (reserveRatio < 0.15) {
      computedMode = "CRITICAL";
    } else if (netWatts < 0) {
      computedMode = "DEFICIT";
    } else if (netWatts > totalProd / 4 && totalProd > 0) {
      computedMode = "SURPLUS";
    }

    if (computedMode !== energyMode) {
      setEnergyMode(computedMode);
      addLog(
        "EnergyModeChanged",
        JSON.stringify({ old_mode: energyMode, new_mode: computedMode, tick: simulationTick + 1 }),
        `ca-sim-${simulationTick + 1}`,
        true
      );
    }
  };

  const handleCreatePlayer = (e: FormEvent) => {
    e.preventDefault();
    if (!newUsername.trim()) return;

    const newUuid = crypto.randomUUID();
    const newPlayer: PlayerSim = {
      uuid: newUuid,
      username: newUsername.trim().toLowerCase(),
      status: "ACTIVE",
      balanceMinor: 0, // Starts at 0
    };

    setPlayers((prev) => [...prev, newPlayer]);
    setNewUsername("");
    addLog(
      "PlayerRegistered",
      JSON.stringify({ player_id: newUuid, username: newPlayer.username }),
      `ca-reg-${newUuid.slice(0, 8)}`
    );
  };

  const handleTransfer = (e: FormEvent) => {
    e.preventDefault();
    if (txFrom === txTo) {
      alert("Source and destination accounts must be distinct.");
      return;
    }

    const minorAmount = Math.round(txAmount * 100);
    if (minorAmount <= 0) return;

    const corrId = `ca-tx-${crypto.randomUUID().slice(0, 8)}`;

    // Simulated Idempotency Protection check
    if (simulateIdempotencyDuplicate && processedRequestIds.has(idempotencyKey)) {
      const response = cachedTransactions[idempotencyKey];
      addLog(
        "DuplicateRequestBlocked",
        JSON.stringify({
          request_id: idempotencyKey,
          message: "Idempotency cache hit! Returned cached transaction details without execution.",
          cached_payload: JSON.parse(response),
        }),
        corrId,
        true
      );
      return;
    }

    const sourcePlayer = players.find((p) => p.uuid === txFrom);
    if (!sourcePlayer) return;

    if (sourcePlayer.balanceMinor < minorAmount) {
      addLog(
        "TransactionFailed",
        JSON.stringify({
          error_code: "INSUFFICIENT_FUNDS",
          required_minor: minorAmount,
          available_minor: sourcePlayer.balanceMinor,
          player_id: txFrom,
        }),
        corrId,
        true
      );
      return;
    }

    // Mutate state atomically (Simulating PgPool transaction)
    setPlayers((prev) =>
      prev.map((p) => {
        if (p.uuid === txFrom) {
          return { ...p, balanceMinor: p.balanceMinor - minorAmount };
        }
        if (p.uuid === txTo) {
          return { ...p, balanceMinor: p.balanceMinor + minorAmount };
        }
        return p;
      })
    );

    const txId = crypto.randomUUID();
    const payload = JSON.stringify({
      transaction_id: txId,
      from: txFrom,
      to: txTo,
      currency: "ASH",
      amount_minor: minorAmount,
    });

    // Save in Idempotency cache
    setProcessedRequestIds((prev) => {
      const next = new Set(prev);
      next.add(idempotencyKey);
      return next;
    });
    setCachedTransactions((prev) => ({
      ...prev,
      [idempotencyKey]: payload,
    }));

    addLog("MoneyTransferred", payload, corrId);

    // Refresh idempotency key unless user wants to simulate resubmitting it
    if (!simulateIdempotencyDuplicate) {
      setIdempotencyKey(crypto.randomUUID());
    }
  };

  const toggleNode = (id: string) => {
    setEnergyNodes((prev) =>
      prev.map((n) => {
        if (n.id === id) {
          const nextState = !n.enabled;
          addLog(
            nextState ? "EnergyNodeOnline" : "EnergyNodeOffline",
            JSON.stringify({ node_id: id, display_name: n.displayName }),
            `ca-node-${id.slice(0, 8)}`,
            true
          );
          return { ...n, enabled: nextState };
        }
        return n;
      })
    );
  };

  const updateNodeWatts = (id: string, watts: number) => {
    setEnergyNodes((prev) =>
      prev.map((n) => {
        if (n.id === id) {
          if (n.type === "PRODUCER") {
            return { ...n, capacityWatts: watts };
          } else {
            return { ...n, capacityWatts: watts };
          }
        }
        return n;
      })
    );
  };

  // =========================================================================
  // SUB-AGGREGATIONS FOR SVG CHARTS
  // =========================================================================

  const totalProdSim = useMemo(() => {
    return energyNodes
      .filter((n) => n.enabled && n.type === "PRODUCER")
      .reduce((acc, curr) => acc + Math.round(curr.capacityWatts * curr.efficiency * curr.health), 0);
  }, [energyNodes]);

  const totalConsSim = useMemo(() => {
    return energyNodes
      .filter((n) => n.enabled && n.type === "CONSUMER")
      .reduce((acc, curr) => acc + curr.capacityWatts, 0);
  }, [energyNodes]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans select-none antialiased">
      {/* =========================================================================
          TOP BANNER: INDUSTRIAL CONTROLLER HUD
          ========================================================================= */}
      <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur-md px-6 py-4 flex flex-col md:flex-row md:items-center md:justify-between shrink-0 gap-4">
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 rounded-lg bg-amber-500/10 border border-amber-500/30 flex items-center justify-center text-amber-500 animate-pulse">
            <Cpu size={22} className="stroke-[1.5]" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-mono text-xs tracking-widest text-amber-500/80 font-semibold uppercase">
                SYSTEM CONTROL PANEL
              </span>
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 inline-block"></span>
            </div>
            <h1 className="text-xl font-semibold tracking-tight text-white flex items-center gap-2">
              STG-Ashland Core
              <span className="font-mono text-xs bg-slate-800 text-slate-400 px-2 py-0.5 rounded border border-slate-700">
                v1.21.1-Core
              </span>
            </h1>
          </div>
        </div>

        {/* Real-time Telemetry Metrics */}
        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 font-mono text-xs text-slate-400 border-l border-slate-800 pl-0 md:pl-6">
          <div className="flex items-center gap-1.5">
            <Database size={13} className="text-emerald-500" />
            <span>PG_POOL:</span>
            <span className="text-white font-semibold">ACTIVE (25 Conns)</span>
          </div>
          <div className="flex items-center gap-1.5">
            <Activity size={13} className="text-amber-500" />
            <span>SIMULATION CYCLE:</span>
            <span className="text-white font-semibold">5000ms</span>
          </div>
          <div className="flex items-center gap-1.5">
            <HardDrive size={13} className="text-indigo-400" />
            <span>UTC TIME:</span>
            <span className="text-slate-200 font-semibold">{systemTime}</span>
          </div>
        </div>
      </header>

      {/* =========================================================================
          TAB NAVIGATION CARDS
          ========================================================================= */}
      <div className="bg-slate-900/45 px-6 border-b border-slate-800/60 flex items-center shrink-0 overflow-x-auto gap-2 py-2">
        <button
          onClick={() => setActiveTab(AppTab.Simulator)}
          className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-mono text-xs tracking-wider uppercase border transition-all duration-200 ${
            activeTab === AppTab.Simulator
              ? "bg-amber-500/10 border-amber-500/40 text-amber-400 shadow-lg shadow-amber-500/5 font-semibold"
              : "bg-transparent border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/50"
          }`}
        >
          <Sliders size={14} />
          🛠️ SYSTEM SIMULATOR
        </button>

        <button
          onClick={() => setActiveTab(AppTab.Workspace)}
          className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-mono text-xs tracking-wider uppercase border transition-all duration-200 ${
            activeTab === AppTab.Workspace
              ? "bg-amber-500/10 border-amber-500/40 text-amber-400 shadow-lg shadow-amber-500/5 font-semibold"
              : "bg-transparent border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/50"
          }`}
        >
          <FileCode size={14} />
          📂 WORKSPACE BROWSER
        </button>

        <button
          onClick={() => setActiveTab(AppTab.Database)}
          className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-mono text-xs tracking-wider uppercase border transition-all duration-200 ${
            activeTab === AppTab.Database
              ? "bg-amber-500/10 border-amber-500/40 text-amber-400 shadow-lg shadow-amber-500/5 font-semibold"
              : "bg-transparent border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/50"
          }`}
        >
          <Database size={14} />
          📊 SQL DATABASE SCHEMA
        </button>

        <button
          onClick={() => setActiveTab(AppTab.Manual)}
          className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-mono text-xs tracking-wider uppercase border transition-all duration-200 ${
            activeTab === AppTab.Manual
              ? "bg-amber-500/10 border-amber-500/40 text-amber-400 shadow-lg shadow-amber-500/5 font-semibold"
              : "bg-transparent border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/50"
          }`}
        >
          <BookOpen size={14} />
          ⚙️ ARCHITECTURE MANUAL
        </button>
      </div>

      {/* =========================================================================
          MAIN CONTAINER: SCROLLABLE VIEWPORTS
          ========================================================================= */}
      <main className="flex-1 overflow-hidden min-h-0 bg-slate-950 p-6">
        <AnimatePresence mode="wait">
          {/* =========================================================================
              TAB: DYNAMIC SYSTEM SIMULATOR (ACTIVE STATE ENGINE)
              ========================================================================= */}
          {activeTab === AppTab.Simulator && (
            <motion.div
              key="simulator"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="h-full grid grid-cols-1 xl:grid-cols-12 gap-6 overflow-y-auto pr-2"
            >
              {/* LEFT & CENTER PANELS: CONTROLS & GAUGES */}
              <div className="xl:col-span-8 flex flex-col gap-6">
                {/* GRID SIMULATION HUD */}
                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
                  <div className="flex items-center justify-between border-b border-slate-800 pb-3 mb-4">
                    <div className="flex items-center gap-2">
                      <Activity className="text-amber-500" size={18} />
                      <h2 className="font-mono text-xs uppercase tracking-wider text-slate-300 font-semibold">
                        Authoritative Energy Simulation Loop
                      </h2>
                    </div>
                    <div className="flex items-center gap-3">
                      <button
                        onClick={() => setIsSimRunning(!isSimRunning)}
                        className={`font-mono text-2xs px-3 py-1 rounded border transition-colors flex items-center gap-1.5 ${
                          isSimRunning
                            ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/25"
                            : "bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200 hover:bg-slate-700"
                        }`}
                      >
                        <Power size={11} />
                        {isSimRunning ? "TICKING (AUTO)" : "PAUSED"}
                      </button>
                      <button
                        onClick={triggerSimTick}
                        className="font-mono text-2xs px-3 py-1 bg-amber-500/10 border border-amber-500/30 text-amber-400 rounded hover:bg-amber-500/20 transition-all flex items-center gap-1.5"
                      >
                        <RefreshCw size={11} />
                        FORCE TICK
                      </button>
                    </div>
                  </div>

                  {/* Telemetry Indicator Bento Cards */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                    <div className="bg-slate-950/60 border border-slate-800 rounded-lg p-3">
                      <span className="font-mono text-3xs text-slate-500 uppercase tracking-wider block">
                        Simulation Tick
                      </span>
                      <span className="font-mono text-lg font-bold text-white">#{simulationTick}</span>
                    </div>

                    <div className="bg-slate-950/60 border border-slate-800 rounded-lg p-3">
                      <span className="font-mono text-3xs text-slate-500 uppercase tracking-wider block">
                        Net Watts Generation
                      </span>
                      <span
                        className={`font-mono text-lg font-bold ${
                          totalProdSim - totalConsSim >= 0 ? "text-emerald-400" : "text-rose-400"
                        }`}
                      >
                        {totalProdSim - totalConsSim >= 0 ? "+" : ""}
                        {(totalProdSim - totalConsSim).toLocaleString()} W
                      </span>
                    </div>

                    <div className="bg-slate-950/60 border border-slate-800 rounded-lg p-3">
                      <span className="font-mono text-3xs text-slate-500 uppercase tracking-wider block">
                        Grid Reserve Capacity
                      </span>
                      <span className="font-mono text-lg font-bold text-white">
                        {globalReserveWh.toLocaleString()} / 1,000,000 Wh
                      </span>
                    </div>

                    <div className="bg-slate-950/60 border border-slate-800 rounded-lg p-3">
                      <span className="font-mono text-3xs text-slate-500 uppercase tracking-wider block">
                        Current Grid Mode
                      </span>
                      <span
                        className={`font-mono text-xs font-bold px-2 py-0.5 rounded border block mt-1 text-center ${
                          energyMode === "NORMAL"
                            ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-400"
                            : energyMode === "SURPLUS"
                            ? "bg-sky-500/10 border-sky-500/20 text-sky-400"
                            : energyMode === "DEFICIT"
                            ? "bg-amber-500/10 border-amber-500/20 text-amber-400"
                            : energyMode === "CRITICAL"
                            ? "bg-orange-500/10 border-orange-500/20 text-orange-400"
                            : "bg-rose-500/10 border-rose-500/20 text-rose-400"
                        }`}
                      >
                        {energyMode}
                      </span>
                    </div>
                  </div>

                  {/* SVG Telemetry visualizer */}
                  <div className="bg-slate-950 border border-slate-800/80 rounded-xl p-4 mb-4 relative overflow-hidden">
                    <div className="absolute top-3 left-4 flex items-center gap-1.5 font-mono text-3xs text-slate-500 uppercase">
                      <TrendingUp size={10} className="text-amber-500" /> Real-time Power Telemetry
                    </div>
                    <div className="h-28 flex items-end gap-1.5 pt-6 select-none">
                      {/* Left scale indicators */}
                      <div className="h-full flex flex-col justify-between font-mono text-3xs text-slate-600 w-12 pb-1 shrink-0 select-none">
                        <span>500k W</span>
                        <span>250k W</span>
                        <span>0 W</span>
                      </div>

                      {/* Custom visual chart drawing */}
                      <div className="flex-1 h-full border-b border-l border-slate-800/70 relative flex items-end justify-around pb-0.5 px-2">
                        {/* Bars mimicking power node sizes */}
                        <div className="w-1/3 bg-slate-900 border border-slate-800 rounded h-full flex flex-col justify-end p-2 relative group overflow-hidden">
                          <div
                            className="bg-sky-500/15 border-t border-sky-400/50 w-full transition-all duration-500"
                            style={{ height: `${(totalProdSim / 500000) * 100}%` }}
                          ></div>
                          <span className="font-mono text-3xs text-sky-400 absolute bottom-1.5 left-2">
                            PROD: {totalProdSim.toLocaleString()} W
                          </span>
                        </div>
                        <div className="w-1/3 bg-slate-900 border border-slate-800 rounded h-full flex flex-col justify-end p-2 relative group overflow-hidden">
                          <div
                            className="bg-amber-500/15 border-t border-amber-400/50 w-full transition-all duration-500"
                            style={{ height: `${(totalConsSim / 500000) * 100}%` }}
                          ></div>
                          <span className="font-mono text-3xs text-amber-400 absolute bottom-1.5 left-2">
                            CONS: {totalConsSim.toLocaleString()} W
                          </span>
                        </div>
                        <div className="w-1/3 bg-slate-900 border border-slate-800 rounded h-full flex flex-col justify-end p-2 relative group overflow-hidden">
                          <div
                            className="bg-indigo-500/15 border-t border-indigo-400/50 w-full transition-all duration-500"
                            style={{ height: `${(globalReserveWh / 1000000) * 100}%` }}
                          ></div>
                          <span className="font-mono text-3xs text-indigo-400 absolute bottom-1.5 left-2">
                            RES: {globalReserveWh.toLocaleString()} Wh
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Minecraft-reported Energy Nodes list */}
                  <div>
                    <h3 className="font-mono text-2xs text-slate-400 uppercase tracking-widest mb-3 flex items-center gap-1">
                      <CornerDownRight size={12} className="text-amber-500" /> Reported Minecraft Infrastructure Nodes
                    </h3>
                    <div className="flex flex-col gap-2.5">
                      {energyNodes.map((node) => (
                        <div
                          key={node.id}
                          className={`border rounded-lg p-3.5 flex flex-col md:flex-row md:items-center md:justify-between transition-colors ${
                            node.enabled
                              ? "bg-slate-950/40 border-slate-800 hover:border-slate-700"
                              : "bg-slate-950/10 border-slate-900 opacity-60"
                          }`}
                        >
                          <div className="flex items-center gap-3">
                            <button
                              onClick={() => toggleNode(node.id)}
                              className={`h-7 w-7 rounded border flex items-center justify-center transition-all ${
                                node.enabled
                                  ? "bg-emerald-500/10 border-emerald-500/40 text-emerald-400 hover:bg-emerald-500/20"
                                  : "bg-rose-500/10 border-rose-500/40 text-rose-400 hover:bg-rose-500/20"
                              }`}
                              title={node.enabled ? "Disable node" : "Enable node"}
                            >
                              <Power size={13} />
                            </button>
                            <div>
                              <div className="flex items-center gap-2">
                                <span className="text-xs font-semibold text-white">{node.displayName}</span>
                                <span className="font-mono text-4xs bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded uppercase">
                                  {node.type}
                                </span>
                              </div>
                              <span className="font-mono text-3xs text-slate-500 uppercase block mt-0.5">
                                ID: {node.id.slice(0, 18)}...
                              </span>
                            </div>
                          </div>

                          {/* Node metrics adjusters */}
                          <div className="flex items-center gap-6 mt-3 md:mt-0 font-mono text-xs w-full md:w-auto justify-between md:justify-end">
                            {node.type === "PRODUCER" && (
                              <div className="flex flex-col gap-1 items-end">
                                <span className="text-3xs text-slate-500">PROD RATE (WATT CAP)</span>
                                <div className="flex items-center gap-2">
                                  <input
                                    type="range"
                                    min="100000"
                                    max="500000"
                                    step="20000"
                                    value={node.capacityWatts}
                                    onChange={(e) => updateNodeWatts(node.id, parseInt(e.target.value))}
                                    className="w-24 accent-amber-500 h-1"
                                    disabled={!node.enabled}
                                  />
                                  <span className="text-slate-200 w-16 text-right">
                                    {Math.round(node.capacityWatts * node.efficiency).toLocaleString()}W
                                  </span>
                                </div>
                              </div>
                            )}

                            {node.type === "CONSUMER" && (
                              <div className="flex flex-col gap-1 items-end">
                                <span className="text-3xs text-slate-500">CONS RATE (WATT LOAD)</span>
                                <div className="flex items-center gap-2">
                                  <input
                                    type="range"
                                    min="50000"
                                    max="400000"
                                    step="10000"
                                    value={node.capacityWatts}
                                    onChange={(e) => updateNodeWatts(node.id, parseInt(e.target.value))}
                                    className="w-24 accent-amber-500 h-1"
                                    disabled={!node.enabled}
                                  />
                                  <span className="text-slate-200 w-16 text-right">
                                    {node.capacityWatts.toLocaleString()}W
                                  </span>
                                </div>
                              </div>
                            )}

                            {node.type === "STORAGE" && (
                              <div className="flex flex-col items-end">
                                <span className="text-3xs text-slate-500">BATTERY RESERVE</span>
                                <span className="text-slate-200 font-semibold block mt-0.5">
                                  {node.storedWh.toLocaleString()} Wh
                                </span>
                              </div>
                            )}

                            {/* Indicators */}
                            <div className="hidden md:flex flex-col items-end text-3xs text-slate-500 gap-0.5">
                              <span>EFF: {(node.efficiency * 100).toFixed(0)}%</span>
                              <span>HLT: {(node.health * 100).toFixed(0)}%</span>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </section>

                {/* DOUBLE ENTRY LEDGER TRANSACTIONS SECTION */}
                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
                  <div className="flex items-center gap-2 border-b border-slate-800 pb-3 mb-4">
                    <Coins className="text-amber-500" size={18} />
                    <h2 className="font-mono text-xs uppercase tracking-wider text-slate-300 font-semibold">
                      Authoritative Ledger Money Terminal
                    </h2>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-12 gap-6">
                    {/* Left: Players and Balances */}
                    <div className="md:col-span-5 border-r border-slate-800/85 pr-0 md:pr-6">
                      <div className="flex items-center justify-between mb-3">
                        <span className="font-mono text-3xs text-slate-400 uppercase tracking-widest block">
                          Registered Player Wallets
                        </span>
                        <span className="font-mono text-4xs bg-amber-500/15 text-amber-400 px-1.5 py-0.5 rounded uppercase">
                          Currency: ASH
                        </span>
                      </div>

                      {/* Players balance listings */}
                      <div className="flex flex-col gap-2 mb-4">
                        {players.map((p) => (
                          <div
                            key={p.uuid}
                            className="bg-slate-950/60 border border-slate-800/80 rounded-lg p-2.5 flex items-center justify-between"
                          >
                            <div className="flex items-center gap-2">
                              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500"></span>
                              <div>
                                <span className="text-xs font-semibold text-slate-200 block">{p.username}</span>
                                <span className="font-mono text-4xs text-slate-500 block">
                                  UUID: {p.uuid.slice(0, 13)}...
                                </span>
                              </div>
                            </div>
                            <div className="text-right font-mono text-xs">
                              <span className="text-white font-bold">
                                {(p.balanceMinor / 100).toLocaleString("en-US", {
                                  minimumFractionDigits: 2,
                                  maximumFractionDigits: 2,
                                })}
                              </span>
                              <span className="text-slate-500 text-3xs ml-1 font-semibold">ASH</span>
                            </div>
                          </div>
                        ))}
                      </div>

                      {/* Add new player form */}
                      <form onSubmit={handleCreatePlayer} className="flex gap-2">
                        <input
                          type="text"
                          value={newUsername}
                          onChange={(e) => setNewUsername(e.target.value)}
                          placeholder="Player username..."
                          className="bg-slate-950 border border-slate-800 text-xs px-3 py-2 rounded-lg flex-1 text-slate-200 focus:outline-none focus:border-amber-500/40"
                        />
                        <button
                          type="submit"
                          className="bg-amber-500/10 hover:bg-amber-500/20 text-amber-400 border border-amber-500/30 font-mono text-xs px-3.5 rounded-lg font-semibold transition-colors flex items-center gap-1"
                        >
                          <UserPlus size={13} />
                          ADD
                        </button>
                      </form>
                    </div>

                    {/* Right: Transfer Terminal form */}
                    <div className="md:col-span-7">
                      <span className="font-mono text-3xs text-slate-400 uppercase tracking-widest block mb-3">
                        Initiate Authoritative Transfer
                      </span>

                      <form onSubmit={handleTransfer} className="flex flex-col gap-3">
                        <div className="grid grid-cols-2 gap-3 font-mono text-xs">
                          <div>
                            <label className="text-3xs text-slate-500 block mb-1">DEBIT ACCOUNT (FROM)</label>
                            <select
                              value={txFrom}
                              onChange={(e) => setTxFrom(e.target.value)}
                              className="bg-slate-950 border border-slate-800 w-full p-2 rounded-lg text-slate-300 focus:outline-none focus:border-amber-500/40"
                            >
                              {players.map((p) => (
                                <option key={p.uuid} value={p.uuid}>
                                  {p.username} ({(p.balanceMinor / 100).toFixed(0)} ASH)
                                </option>
                              ))}
                            </select>
                          </div>
                          <div>
                            <label className="text-3xs text-slate-500 block mb-1">CREDIT ACCOUNT (TO)</label>
                            <select
                              value={txTo}
                              onChange={(e) => setTxTo(e.target.value)}
                              className="bg-slate-950 border border-slate-800 w-full p-2 rounded-lg text-slate-300 focus:outline-none focus:border-amber-500/40"
                            >
                              {players.map((p) => (
                                <option key={p.uuid} value={p.uuid}>
                                  {p.username} ({(p.balanceMinor / 100).toFixed(0)} ASH)
                                </option>
                              ))}
                            </select>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <label className="font-mono text-3xs text-slate-500 block mb-1">AMOUNT (WHOLE COINS)</label>
                            <div className="relative">
                              <input
                                type="number"
                                min="1"
                                max="10000"
                                value={txAmount}
                                onChange={(e) => setTxAmount(parseFloat(e.target.value) || 0)}
                                className="bg-slate-950 border border-slate-800 font-mono text-xs w-full px-3 py-2 pr-10 rounded-lg text-slate-200 focus:outline-none focus:border-amber-500/40"
                              />
                              <span className="font-mono text-3xs text-slate-500 absolute right-3 top-2.5">ASH</span>
                            </div>
                          </div>
                          <div>
                            <label className="font-mono text-3xs text-slate-500 block mb-1">IDEMPOTENCY KEY</label>
                            <div className="flex gap-1.5">
                              <input
                                type="text"
                                readOnly
                                value={`${idempotencyKey.slice(0, 15)}...`}
                                className="bg-slate-950/70 border border-slate-900 font-mono text-3xs px-2.5 py-2.5 rounded-lg flex-1 text-slate-500 cursor-not-allowed select-all"
                              />
                              <button
                                type="button"
                                onClick={() => setIdempotencyKey(crypto.randomUUID())}
                                className="border border-slate-800 text-slate-400 hover:text-slate-200 hover:bg-slate-800 px-2.5 rounded-lg transition-colors"
                                title="Regenerate key"
                              >
                                <RefreshCw size={11} />
                              </button>
                            </div>
                          </div>
                        </div>

                        {/* Double-entry preview visualizer */}
                        <div className="bg-slate-950 border border-slate-800 p-3 rounded-xl font-mono text-2xs text-slate-400 mt-1">
                          <span className="text-3xs text-slate-500 uppercase tracking-wider block mb-2">
                            PostgreSQL Transaction Ledger Preview
                          </span>
                          <div className="flex flex-col gap-1.5">
                            <div className="flex items-center justify-between text-rose-400 bg-rose-500/5 px-2 py-1 rounded">
                              <div className="flex items-center gap-1.5">
                                <CornerDownRight size={10} />
                                <span>debit entry (source wallet)</span>
                              </div>
                              <span className="font-bold">-{txAmount.toFixed(2)} ASH</span>
                            </div>
                            <div className="flex items-center justify-between text-emerald-400 bg-emerald-500/5 px-2 py-1 rounded">
                              <div className="flex items-center gap-1.5">
                                <CornerDownRight size={10} />
                                <span>credit entry (target wallet)</span>
                              </div>
                              <span className="font-bold">+{txAmount.toFixed(2)} ASH</span>
                            </div>
                            <div className="h-px bg-slate-800/80 my-1"></div>
                            <div className="flex items-center justify-between font-bold text-slate-300 px-2">
                              <span>DOUBLE-ENTRY ACCOUNTING CHECK</span>
                              <span className="text-emerald-500">SUM = 0.00 (OK)</span>
                            </div>
                          </div>
                        </div>

                        {/* Idempotency simulation selector */}
                        <div className="flex items-center justify-between mt-1">
                          <label className="flex items-center gap-2 cursor-pointer group">
                            <input
                              type="checkbox"
                              checked={simulateIdempotencyDuplicate}
                              onChange={(e) => setSimulateIdempotencyDuplicate(e.target.checked)}
                              className="accent-amber-500 h-3.5 w-3.5 bg-slate-950 border border-slate-800 rounded"
                            />
                            <span className="font-mono text-3xs text-slate-400 uppercase tracking-wider select-none group-hover:text-slate-200">
                              Simulate Duplicate Request ID (Retry)
                            </span>
                          </label>

                          <button
                            type="submit"
                            className="bg-amber-500 text-slate-950 font-mono text-xs px-5 py-2 rounded-lg font-bold hover:bg-amber-400 transition-all shadow-md shadow-amber-500/15 flex items-center gap-1.5"
                          >
                            <ArrowRightLeft size={13} />
                            COMMIT TRANSACTION
                          </button>
                        </div>
                      </form>
                    </div>
                  </div>
                </section>
              </div>

              {/* RIGHT PANEL: STG EVENT QUEUE LOGGER */}
              <div className="xl:col-span-4 flex flex-col min-h-[500px] xl:h-full bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
                {/* Header */}
                <div className="bg-slate-950 border-b border-slate-800/80 px-4 py-3 flex items-center justify-between shrink-0">
                  <div className="flex items-center gap-2">
                    <Terminal size={15} className="text-amber-500 animate-pulse" />
                    <span className="font-mono text-xs font-semibold tracking-wider text-slate-200">
                      STG EVENT QUEUE STREAM
                    </span>
                  </div>
                  <span className="font-mono text-4xs bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 px-2 py-0.5 rounded uppercase">
                    NATS LINKED
                  </span>
                </div>

                {/* Scrolling Logs list */}
                <div className="flex-1 p-4 overflow-y-auto font-mono text-3xs space-y-3 scrollbar-thin select-text">
                  <AnimatePresence initial={false}>
                    {logs.map((log) => (
                      <motion.div
                        key={log.id}
                        initial={{ opacity: 0, x: 20 }}
                        animate={{ opacity: 1, x: 0 }}
                        className={`p-2.5 rounded border border-l-3 bg-slate-950/60 ${
                          log.isSystem
                            ? "border-amber-500/30 border-l-amber-500"
                            : "border-indigo-500/30 border-l-indigo-500"
                        }`}
                      >
                        <div className="flex items-center justify-between text-slate-500 font-semibold mb-1">
                          <span className="text-white">
                            [{log.timestamp}] EVENT: {log.type}
                          </span>
                          <span>{log.id.split("-")[1]}</span>
                        </div>
                        <div className="text-slate-300 break-all bg-slate-950 p-1.5 rounded border border-slate-900/60 font-mono select-all">
                          {log.payload}
                        </div>
                        <div className="flex items-center gap-1 text-slate-600 font-bold mt-1.5 uppercase select-none">
                          <span>Correlation ID:</span>
                          <span className="text-slate-400 select-all">{log.correlationId}</span>
                        </div>
                      </motion.div>
                    ))}
                  </AnimatePresence>
                </div>
              </div>
            </motion.div>
          )}

          {/* =========================================================================
              TAB: Cargo WORKSPACE BROWSER (CODE VIEWER)
              ========================================================================= */}
          {activeTab === AppTab.Workspace && (
            <motion.div
              key="workspace"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="h-full grid grid-cols-1 md:grid-cols-12 gap-6"
            >
              {/* File Tree Selector Panel */}
              <div className="md:col-span-3 bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-col overflow-hidden">
                <span className="font-mono text-3xs text-slate-500 uppercase tracking-widest block mb-3 font-semibold">
                  STG-Core Cargo Workspace
                </span>

                <div className="flex-1 overflow-y-auto flex flex-col gap-1.5">
                  <div className="text-slate-400 flex items-center gap-1.5 font-mono text-2xs py-1 select-none">
                    <ChevronRight size={12} className="rotate-90 text-amber-500" />
                    <span>stg-backend /</span>
                  </div>

                  {rustWorkspaceFiles.map((file) => {
                    const isSelected = selectedFile.path === file.path;
                    return (
                      <button
                        key={file.path}
                        onClick={() => setSelectedFile(file)}
                        className={`flex items-center justify-between text-left px-3 py-2 rounded-lg transition-all border ${
                          isSelected
                            ? "bg-amber-500/10 border-amber-500/20 text-amber-400"
                            : "bg-transparent border-transparent text-slate-400 hover:bg-slate-800/40 hover:text-slate-200"
                        }`}
                      >
                        <div className="flex items-center gap-2">
                          <FileCode size={14} className={isSelected ? "text-amber-500" : "text-slate-500"} />
                          <div className="font-mono text-xs font-semibold">
                            {file.name}
                          </div>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Code viewer & Explanatory Details */}
              <div className="md:col-span-9 flex flex-col gap-4 overflow-hidden h-full">
                <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 flex-1 flex flex-col min-h-0 relative overflow-hidden">
                  <div className="bg-slate-950 border-b border-slate-800 px-4 py-2 flex items-center justify-between shrink-0 font-mono text-2xs select-none">
                    <span className="text-slate-400 font-semibold">{selectedFile.path}</span>
                    <span className="bg-slate-800 text-slate-400 px-2 py-0.5 rounded uppercase">
                      RUST / STRICT TYPE SAFETY
                    </span>
                  </div>

                  {/* Code Block display */}
                  <div className="flex-1 overflow-auto bg-slate-950/80 p-4 font-mono text-xs text-slate-300 leading-relaxed scrollbar-thin select-text">
                    <pre className="whitespace-pre">
                      {selectedFile.code}
                    </pre>
                  </div>
                </div>

                {/* Structural architecture notes */}
                <div className="bg-slate-900/50 border border-slate-800/80 rounded-xl p-4 flex flex-col md:flex-row md:items-center justify-between gap-4 shrink-0">
                  <div className="flex items-start gap-3">
                    <FileText className="text-amber-500 shrink-0 mt-0.5" size={18} />
                    <div>
                      <h4 className="text-xs font-semibold text-white uppercase font-mono tracking-wider">
                        Architectural Verification Notes
                      </h4>
                      <p className="text-xs text-slate-400 leading-relaxed mt-1">
                        {selectedFile.description}
                      </p>
                    </div>
                  </div>

                  {/* Highlight badges */}
                  <div className="flex flex-col gap-1 shrink-0 font-mono text-3xs w-full md:w-80">
                    <span className="text-slate-500 uppercase tracking-widest font-semibold block mb-1">
                      Cyclic-Dependency Safeguards:
                    </span>
                    {selectedFile.highlights.map((h, i) => (
                      <div key={i} className="flex items-center gap-1.5 text-slate-300">
                        <CheckCircle2 size={10} className="text-emerald-500 shrink-0" />
                        <span>{h}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </motion.div>
          )}

          {/* =========================================================================
              TAB: RELATIONAL DATABASE SCHEMA VISUALIZER
              ========================================================================= */}
          {activeTab === AppTab.Database && (
            <motion.div
              key="database"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="h-full grid grid-cols-1 md:grid-cols-12 gap-6"
            >
              {/* Tables list selector */}
              <div className="md:col-span-4 bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-col overflow-y-auto">
                <span className="font-mono text-3xs text-slate-500 uppercase tracking-widest block mb-3 font-semibold">
                  STG-Core SQLx PostgreSQL Schema
                </span>

                <div className="flex flex-col gap-2">
                  {dbSchemaTables.map((table) => {
                    const isSelected = selectedTable.name === table.name;
                    return (
                      <button
                        key={table.name}
                        onClick={() => setSelectedTable(table)}
                        className={`text-left p-3.5 rounded-xl border transition-all ${
                          isSelected
                            ? "bg-amber-500/10 border-amber-500/20 text-amber-400"
                            : "bg-slate-950/40 border-slate-800/80 text-slate-400 hover:border-slate-700 hover:text-slate-200"
                        }`}
                      >
                        <div className="flex items-center gap-2 mb-1.5">
                          <Database size={14} className={isSelected ? "text-amber-500" : "text-slate-500"} />
                          <span className="font-mono text-sm font-semibold text-white">
                            {table.name}
                          </span>
                        </div>
                        <p className="text-xs text-slate-400 leading-relaxed">
                          {table.description}
                        </p>
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Table Column detail visualizer */}
              <div className="md:col-span-8 bg-slate-900 border border-slate-800 rounded-xl p-5 flex flex-col overflow-hidden h-full">
                <div className="flex items-center justify-between border-b border-slate-800 pb-3 mb-4 shrink-0">
                  <div className="flex items-center gap-2">
                    <Database className="text-amber-500" size={16} />
                    <h3 className="font-mono text-sm font-semibold text-slate-200">
                      Table Definition: <span className="text-amber-400">{selectedTable.name}</span>
                    </h3>
                  </div>
                  <span className="font-mono text-3xs bg-slate-800 text-slate-400 px-2 py-0.5 rounded">
                    POSTGRESQL DIALECT
                  </span>
                </div>

                {/* Columns Visual Grid */}
                <div className="flex-1 overflow-y-auto space-y-3">
                  <div className="grid grid-cols-12 gap-3 px-3 py-1.5 font-mono text-3xs text-slate-500 uppercase tracking-wider select-none shrink-0 border-b border-slate-800/60 pb-2">
                    <div className="col-span-4">COLUMN NAME</div>
                    <div className="col-span-3">DATA TYPE</div>
                    <div className="col-span-5">CONSTRAINTS & FOREIGN KEYS</div>
                  </div>

                  <div className="space-y-1.5 select-text">
                    {selectedTable.columns.map((col) => (
                      <div
                        key={col.name}
                        className="grid grid-cols-12 gap-3 bg-slate-950/60 hover:bg-slate-950 border border-slate-800/70 p-3 rounded-lg font-mono text-xs items-center"
                      >
                        <div className="col-span-4 font-bold text-slate-200">{col.name}</div>
                        <div className="col-span-3 text-amber-500 font-semibold">{col.type}</div>
                        <div className="col-span-5 text-slate-400 text-2xs leading-relaxed">
                          {col.constraints || "—"}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Integrity assertions card */}
                <div className="bg-slate-950 border border-slate-800 rounded-xl p-4 mt-4 shrink-0 flex items-start gap-3">
                  <AlertCircle size={18} className="text-amber-500 mt-0.5 shrink-0" />
                  <div>
                    <h4 className="font-mono text-2xs uppercase text-white font-semibold">
                      Authoritative SQL Integrity Assertion
                    </h4>
                    <p className="text-xs text-slate-400 leading-relaxed mt-1">
                      Our schema completely avoids ORM-hidden layers. Standard constraints like check triggers (e.g., balance &gt;= 0) and unique composite indexes are locked down directly in Postgres. This enforces financial safety rules even in the event of concurrent service updates or infrastructure restarts.
                    </p>
                  </div>
                </div>
              </div>
            </motion.div>
          )}

          {/* =========================================================================
              TAB: STG-ASHLAND ARCHITECTURE MANUAL
              ========================================================================= */}
          {activeTab === AppTab.Manual && (
            <motion.div
              key="manual"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="h-full overflow-y-auto space-y-6 pr-2 select-text"
            >
              {/* Architecture Principles Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
                  <div className="flex items-center gap-2 mb-3 text-amber-500">
                    <Layers size={18} />
                    <h3 className="font-mono text-sm uppercase tracking-wider font-semibold text-slate-200">
                      Domain-Oriented Architecture
                    </h3>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    STG-Core is designed as a **modular monolith** with strongly decoupled internal domains. The core domains (Player, Wallet, Energy, World, Transition) are isolated inside their respective crates.
                  </p>
                  <ul className="list-disc list-inside mt-3 space-y-1.5 font-mono text-2xs text-slate-300 border-t border-slate-800 pt-3">
                    <li><span className="font-bold text-amber-400">stg-domain:</span> Pure models. Absolutely zero tonic or sqlx dependencies.</li>
                    <li><span className="font-bold text-amber-400">stg-application:</span> Declares input/output ports (traits) and executes transactions.</li>
                    <li><span className="font-bold text-amber-400">stg-infrastructure:</span> Holds technical SQLx database queries and queue publisher adapters.</li>
                  </ul>
                </section>

                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
                  <div className="flex items-center gap-2 mb-3 text-amber-500">
                    <ShieldAlert size={18} />
                    <h3 className="font-mono text-sm uppercase tracking-wider font-semibold text-slate-200">
                      Robust Error Model (No Unwrap)
                    </h3>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    To maintain absolute uptime and crash-resiliency in high-load server environments, **no unwrap() statements exist in the production-ready request paths**.
                  </p>
                  <p className="text-xs text-slate-400 leading-relaxed mt-2">
                    Every operation returns a robust Rust `Result&lt;T, DomainError&gt;`. Error cases (such as concurrent modifications, insufficient wallet funds, or expired resource reservations) are captured cleanly, serialized, and returned to modded Minecraft clients via gRPC.
                  </p>
                </section>

                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
                  <div className="flex items-center gap-2 mb-3 text-amber-500">
                    <Coins size={18} />
                    <h3 className="font-mono text-sm uppercase tracking-wider font-semibold text-slate-200">
                      Authoritative Ledger Money model
                    </h3>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    Money balances must never be blindly incremented or decremented without producing structured double-entry entries. For every balance mutation, an `EconomyTransaction` is recorded.
                  </p>
                  <p className="text-xs text-slate-400 leading-relaxed mt-2 font-mono text-slate-300">
                    Money represents fractional precision strictly through <span className="text-amber-400 font-bold">i64 minor units</span>. This completely eliminates any risk of floating-point drift or roundoff errors. 1.00 credit is stored in the database strictly as 100 minor units.
                  </p>
                </section>

                <section className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg">
                  <div className="flex items-center gap-2 mb-3 text-amber-500">
                    <Settings size={18} />
                    <h3 className="font-mono text-sm uppercase tracking-wider font-semibold text-slate-200">
                      Idempotent Command Execution
                    </h3>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    Network requests between modded Minecraft servers and the core core backend might fail due to standard connection drops, causing duplicate command retries.
                  </p>
                  <p className="text-xs text-slate-400 leading-relaxed mt-2">
                    The backend protects balances by storing processed unique `request_id`s. In the event of a duplicate submission, the backend skips duplicate execution, fetches the cached original response payload from the `processed_requests` table, and returns it safely.
                  </p>
                </section>
              </div>

              {/* Developer notice banner */}
              <div className="bg-amber-500/10 border border-amber-500/20 p-5 rounded-xl flex items-start gap-3">
                <CheckCircle2 className="text-amber-500 shrink-0 mt-0.5" size={20} />
                <div>
                  <h4 className="font-mono text-xs uppercase text-amber-400 font-bold">
                    Architect Verification Complete
                  </h4>
                  <p className="text-xs text-slate-300 leading-relaxed mt-1">
                    The Cargo workspace folder structure, database schema layout, Protobuf gRPC contracts, and domain types have been meticulously designed and generated in the root directory. You can export the workspace (via ZIP/GitHub settings) at any time to build and run the Rust core directly!
                  </p>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
}
