package io.github.immaghzbad.aetherst.core

import io.github.immaghzbad.aetherst.platform.PlatformContext
import io.github.immaghzbad.aetherst.shared.data.LogRepository
import io.github.immaghzbad.aetherst.shared.model.AetherConfig
import java.io.File
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicLong

object TorController {
    @Volatile private var torPort: Int = 3081
    @Volatile private var running = false
    @Volatile private var coreAlive = false
    @Volatile private var connected = false
    @Volatile private var bootstrapPercent: Int = 0
    @Volatile private var torSocksListening: Boolean = false
    private val lastEventTime = AtomicLong(0L)

    fun isSupported(config: AetherConfig): Boolean {
        return config.torEnabled
    }

    fun getUpstreamProxy(): String {
        return "socks5://127.0.0.1:$torPort"
    }

    fun getTorBindAddress(): String {
        return "127.0.0.1:$torPort"
    }

    fun effectiveTorPort(config: AetherConfig): Int {
        val requested = config.torBindPort.toIntOrNull() ?: 3081
        if (requested.toString() == config.socksPort || requested.toString() == config.httpPort || requested.toString() == config.psiphonSocksPort) {
            return findFreePort()
        }
        return requested
    }

    fun activePort(config: AetherConfig): Int = if (running) torPort else effectiveTorPort(config)

    private fun findFreePort(): Int {
        return try {
            ServerSocket(0).use { it.localPort }
        } catch (_: Exception) { 3081 }
    }

    private fun reserveTorPort(config: AetherConfig): Int {
        val requested = config.torBindPort.toIntOrNull() ?: 3081
        if (requested.toString() == config.socksPort || requested.toString() == config.httpPort || requested.toString() == config.psiphonSocksPort) {
            return findFreePort()
        }
        return try {
            ServerSocket(requested).use { }
            requested
        } catch (_: Exception) {
            findFreePort()
        }
    }

    fun start(context: PlatformContext, config: AetherConfig): Boolean {
        if (!isSupported(config)) return false
        return try {
            val dir = File(io.github.immaghzbad.aetherst.platform.getSystemUtils(context).getFilesDir(), "tor")
            if (!dir.exists() && !dir.mkdirs()) {
                LogRepository.e("Tor state dir unavailable", "Tor")
                running = false
                coreAlive = false
                connected = false
                return false
            }
            torPort = reserveTorPort(config)
            connected = false
            coreAlive = false
            bootstrapPercent = 0
            torSocksListening = false
            lastEventTime.set(System.currentTimeMillis())
            running = true
            LogRepository.i("Tor prepared on 127.0.0.1:$torPort mode=${config.torMode.rawValue}", "Tor")
            true
        } catch (e: Exception) {
            val reason = e.message ?: "unknown error"
            LogRepository.e("Tor start exception: $reason", "Tor")
            running = false
            coreAlive = false
            connected = false
            false
        }
    }

    fun notifyCoreReady() {
        if (!running) return
        coreAlive = true
        lastEventTime.set(System.currentTimeMillis())
        LogRepository.i("Tor core alive", "Tor")
    }

    fun notifyCoreDead() {
        coreAlive = false
        connected = false
        bootstrapPercent = 0
        torSocksListening = false
    }

    fun notifyBootstrap(percent: Int) {
        if (!running) return
        if (percent > bootstrapPercent) {
            bootstrapPercent = percent
            lastEventTime.set(System.currentTimeMillis())
        }
        if (percent >= 100) {
            coreAlive = true
        }
    }

    fun notifySocksListening(port: Int = torPort) {
        torPort = port
        torSocksListening = true
        lastEventTime.set(System.currentTimeMillis())
        LogRepository.i("Tor socks5 listening on 127.0.0.1:$port", "Tor")
    }

    fun currentPort(): Int = torPort

    fun isFullyReady(stableMs: Long): Boolean = isConnected() && torSocksListening && bootstrapPercent >= 100 && stableFor(stableMs)

    fun notifyProxyReady(port: Int = torPort) {
        if (connected && torPort == port) return
        torPort = port
        connected = true
        lastEventTime.set(System.currentTimeMillis())
        LogRepository.i("Tor proxy ready on 127.0.0.1:$port", "Tor")
    }

    fun protectFd(fd: Int): Boolean {
        return true
    }

    fun stop() {
        if (!running && !connected && !coreAlive) return
        running = false
        coreAlive = false
        connected = false
        bootstrapPercent = 0
        torSocksListening = false
        lastEventTime.set(0L)
        LogRepository.i("Tor stopped", "Tor")
    }

    fun cleanStaleLocks(context: PlatformContext) {
        try {
            val dir = File(io.github.immaghzbad.aetherst.platform.getSystemUtils(context).getFilesDir(), "tor")
            dir.listFiles { f -> f.isFile && f.name.endsWith(".lock") }?.forEach {
                runCatching { it.delete() }
            }
        } catch (_: Exception) {}
    }

    fun isPrepared(): Boolean = running

    fun isRunning(): Boolean = running && coreAlive

    fun isConnected(): Boolean = connected && running && coreAlive

    fun stableFor(graceMs: Long): Boolean = connected && running && coreAlive &&
            (System.currentTimeMillis() - lastEventTime.get()) >= graceMs
}
