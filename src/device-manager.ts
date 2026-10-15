/**
 * DeviceManager
 *
 * Manages lookin-mcp connection targets (simulator or USB physical device).
 *
 * Physical device mode: discovers LookinServer in its HTTP port range, then
 * starts an ephemeral local TCP proxy tunneled to that device port via usbmuxd.
 * client.ts fetch logic requires no changes.
 *
 * Dependency: macOS built-in usbmuxd (/var/run/usbmuxd), zero external tool dependencies.
 */

import * as net from "net";
import { execSync } from "child_process";
import { listUsbDevices, connectToDevice, watchDevices } from "./usbmuxd.js";

const LOOKINSERVER_PORT_START = 47190;
const LOOKINSERVER_PORT_END = 47199;
const PORT_PROBE_TIMEOUT_MS = 500;

export type ConnectionTarget =
  | { type: "simulator" }
  | { type: "device"; udid: string };

export interface DeviceInfo {
  udid: string;
  name: string;
  type: "physical" | "simulator";
}

class DeviceManager {
  private _activeTarget: ConnectionTarget = { type: "simulator" };
  private _proxyServer: net.Server | null = null;
  private _proxyPort: number | null = null;
  private _simulatorServerPort: number | null = null;
  private _activeDeviceId: number | null = null;
  private _needsDeviceSelection = false;
  private _stopWatch: (() => void) | null = null;
  /** deviceId → udid map, kept in sync by watchDevices */
  private _deviceIdMap = new Map<number, string>();

  async getBaseUrl(): Promise<string> {
    if (this._activeTarget.type === "device") {
      if (this._proxyPort === null) {
        if (this._activeDeviceId === null) {
          throw new Error("No physical device is connected.");
        }
        await this._discoverAndStartDeviceProxy(this._activeDeviceId);
      }
      return `http://127.0.0.1:${this._proxyPort}`;
    }

    if (this._simulatorServerPort === null) {
      this._simulatorServerPort = await this._discoverSimulatorServerPort();
    }
    return `http://127.0.0.1:${this._simulatorServerPort}`;
  }

  getActiveTarget(): ConnectionTarget {
    return this._activeTarget;
  }

  needsDeviceSelection(): boolean {
    return this._needsDeviceSelection;
  }

  /** Called at startup: auto-connect if exactly 1 device, otherwise flag for manual selection. */
  async autoConnect(): Promise<void> {
    const devices = await this.listDevices();
    if (devices.length === 1) {
      const device = devices[0];
      if (device.type === "physical") {
        await this.connectDevice(device.udid);
      }
      this._needsDeviceSelection = false;
    } else if (devices.length > 1) {
      this._needsDeviceSelection = true;
    }

    // Start watching for device connect/disconnect events
    if (!this._stopWatch) {
      watchDevices((event) => {
        if (event.type === "attached" && event.udid) {
          this._deviceIdMap.set(event.deviceId, event.udid);
        } else if (event.type === "detached") {
          const detachedUdid = this._deviceIdMap.get(event.deviceId);
          this._deviceIdMap.delete(event.deviceId);

          // If the active physical device was disconnected, re-run auto-connect
          if (
            this._activeTarget.type === "device" &&
            detachedUdid === this._activeTarget.udid
          ) {
            process.stderr.write(`[lookin-mcp] Device ${detachedUdid} disconnected, re-connecting...\n`);
            this._stopProxy().then(() => {
              this._activeTarget = { type: "simulator" };
              this.autoConnect().catch(() => { /* non-fatal */ });
            });
          }
        }
      }).then((stop) => {
        this._stopWatch = stop;
      }).catch(() => { /* usbmuxd watch unavailable, skip */ });
    }
  }

  /** List iOS devices connected via USB and running simulators. */
  async listDevices(): Promise<DeviceInfo[]> {
    const [physicalDevices, simulators] = await Promise.all([
      listUsbDevices().then((devices) =>
        devices.map((d) => ({
          udid: d.udid,
          name: d.udid,
          type: "physical" as const,
        }))
      ),
      this._listBootedSimulators(),
    ]);
    return [...physicalDevices, ...simulators];
  }

  private _listBootedSimulators(): DeviceInfo[] {
    try {
      const output = execSync("xcrun simctl list devices booted --json", { encoding: "utf-8" });
      const data = JSON.parse(output) as {
        devices: Record<string, Array<{ udid: string; name: string; state: string }>>;
      };
      const result: DeviceInfo[] = [];
      for (const devices of Object.values(data.devices)) {
        for (const dev of devices) {
          if (dev.state === "Booted") {
            result.push({ udid: dev.udid, name: dev.name, type: "simulator" });
          }
        }
      }
      return result;
    } catch {
      return [];
    }
  }

  async connectSimulator(): Promise<void> {
    await this._stopProxy();
    this._activeDeviceId = null;
    this._simulatorServerPort = null;
    this._activeTarget = { type: "simulator" };
    this._needsDeviceSelection = false;
  }

  async connectDevice(udid: string): Promise<void> {
    // Check if the UDID belongs to a booted simulator
    const simulators = this._listBootedSimulators();
    if (simulators.some((s) => s.udid === udid)) {
      await this.connectSimulator();
      return;
    }

    const devices = await listUsbDevices();
    const dev = devices.find((d) => d.udid === udid);
    if (!dev) {
      throw new Error(
        `Device ${udid} not found. Make sure it is connected via USB and trusted on this Mac.`
      );
    }

    await this._stopProxy();
    this._activeDeviceId = dev.deviceId;
    this._activeTarget = { type: "device", udid };
    this._needsDeviceSelection = false;
  }

  /**
   * Start an ephemeral local TCP proxy. Each incoming connection is tunneled
   * to the selected LookinServer port on the device via usbmuxd.
   */
  private _startProxy(deviceId: number, devicePort: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const server = net.createServer((clientSock) => {
        connectToDevice(deviceId, devicePort)
          .then((tunnel) => {
            clientSock.pipe(tunnel);
            tunnel.pipe(clientSock);
            clientSock.on("error", () => tunnel.destroy());
            clientSock.on("close", () => tunnel.destroy());
            tunnel.on("error", () => clientSock.destroy());
            tunnel.on("close", () => clientSock.destroy());
          })
          .catch(() => {
            clientSock.destroy();
          });
      });

      this._proxyServer = server;

      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        if (!address || typeof address === "string") {
          reject(new Error("Failed to determine the local proxy port."));
          return;
        }
        this._proxyPort = address.port;
        resolve();
      });

      server.on("error", (err: NodeJS.ErrnoException) => {
        this._proxyServer = null;
        this._proxyPort = null;
        reject(err);
      });
    });
  }

  private async _discoverSimulatorServerPort(): Promise<number> {
    const candidates = await Promise.all(
      this._serverPorts().map(async (port) => ({
        port,
        status: await this._probeStatus(`http://127.0.0.1:${port}`),
      }))
    );
    const servers = candidates.filter((candidate) => candidate.status !== null);
    servers.sort((a, b) =>
      Number(Boolean(b.status?.active)) - Number(Boolean(a.status?.active)) || a.port - b.port
    );
    if (servers.length === 0) {
      throw new Error(
        `Cannot find LookinServer on ports ${LOOKINSERVER_PORT_START}-${LOOKINSERVER_PORT_END}.`
      );
    }
    return servers[0].port;
  }

  private async _discoverAndStartDeviceProxy(deviceId: number): Promise<void> {
    for (const devicePort of this._serverPorts()) {
      await this._startProxy(deviceId, devicePort);
      const baseUrl = `http://127.0.0.1:${this._proxyPort}`;
      if (await this._probeStatus(baseUrl)) {
        return;
      }
      await this._stopProxy();
    }
    throw new Error(
      `Cannot find LookinServer on device ports ${LOOKINSERVER_PORT_START}-${LOOKINSERVER_PORT_END}.`
    );
  }

  private _serverPorts(): number[] {
    return Array.from(
      { length: LOOKINSERVER_PORT_END - LOOKINSERVER_PORT_START + 1 },
      (_, index) => LOOKINSERVER_PORT_START + index
    );
  }

  private async _probeStatus(baseUrl: string): Promise<{ active?: boolean } | null> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), PORT_PROBE_TIMEOUT_MS);
    try {
      const response = await fetch(`${baseUrl}/status`, { signal: controller.signal });
      if (!response.ok) return null;
      const json = await response.json() as {
        success?: boolean;
        data?: { active?: boolean };
      };
      // LookinServer tags 1.0.0/1.0.1 don't return a "protocol" field in /status yet;
      // requiring it made device discovery always fail. Accept any successful status.
      if (!json.success || !json.data) return null;
      return json.data;
    } catch {
      return null;
    } finally {
      clearTimeout(timeout);
    }
  }

  private _stopProxy(): Promise<void> {
    return new Promise((resolve) => {
      if (!this._proxyServer) {
        this._proxyPort = null;
        resolve();
        return;
      }
      this._proxyServer.close(() => resolve());
      this._proxyServer = null;
      this._proxyPort = null;
    });
  }

  async shutdown(): Promise<void> {
    this._stopWatch?.();
    this._stopWatch = null;
    await this._stopProxy();
  }
}

export const deviceManager = new DeviceManager();
