package io.github.immaghzbad.aetherst.shared.core

import io.github.immaghzbad.aetherst.shared.data.LogRepository
import io.github.immaghzbad.aetherst.shared.model.*
import io.github.immaghzbad.aetherst.platform.PlatformContext
import io.github.immaghzbad.aetherst.platform.getSystemUtils
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds

class AetherProcessRunner(private val context: PlatformContext) {
    private val systemUtils = getSystemUtils(context)
    private var process: PlatformProcess? = null
    private var runnerJob: Job? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val currentAttemptId = AtomicLong(0)
    private var goolOuterValidated = false
    private var lastBindAddress: String = "127.0.0.1:1819"

    private suspend fun runCommand(vararg command: String): Int = withContext(Dispatchers.IO) {
        LogRepository.d("Executing command: ${command.joinToString(" ")}")
        try {
            val pb = ProcessBuilder(*command)
            pb.redirectErrorStream(true)
            val proc = pb.start()
            val output = StringBuilder()
            proc.inputStream.bufferedReader().use { reader ->
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    output.append(line).append("\n")
                }
            }
            val exitCode = proc.waitFor()
            val finalOutput = output.toString().trim()
            if (finalOutput.isNotEmpty()) {
                LogRepository.i("Command '${command[0]}' output:\n$finalOutput")
            }
            if (exitCode != 0) {
                LogRepository.w("Command '${command[0]}' failed with exit code $exitCode")
            }
            exitCode
        } catch (e: Exception) {
            LogRepository.e("Critical error executing ${command[0]}: ${e.message}")
            -1
        }
    }

    private val _connectionStatus = MutableStateFlow(ConnectionStatus.STOPPED)
    val connectionStatus: StateFlow<ConnectionStatus> = _connectionStatus.asStateFlow()

    fun start(config: AetherConfig, bindAddress: String, onCodeRequired: () -> Unit = {}, inputProvider: suspend () -> String = { "" }) {
        if (runnerJob?.isActive == true) return
        LogRepository.currentAppLogLevel = config.appLogLevel
        LogRepository.currentCoreLogLevel = config.coreLogLevel
        val attemptId = currentAttemptId.incrementAndGet()
        updateState(ConnectionStatus.STARTING, attemptId)
        runnerJob = scope.launch {
            var retryCount = 0
            while (isActive && (currentAttemptId.get() == attemptId)) {
                if (!config.smartReconnect && retryCount > 0) {
                    LogRepository.e("Smart Reconnect disabled -> stopping retry")
                    updateState(ConnectionStatus.ERROR, attemptId)
                    break
                }
                if (config.smartReconnect && retryCount >= config.reconnectRetryLimit) {
                    LogRepository.e("Smart Reconnect limit reached ($retryCount). Stopping...")
                    updateState(ConnectionStatus.ERROR, attemptId)
                    break
                }
                if (retryCount > 0) {
                    val waitTime = (retryCount * 1000L).coerceAtMost(10000L)
                    LogRepository.i("Recovering connection (Retry $retryCount)...")
                    updateState(ConnectionStatus.RECONNECTING, attemptId)
                    delay(waitTime.milliseconds)
                } else {
                    LogRepository.i("Starting system core at $bindAddress")
                    lastBindAddress = bindAddress
                }
                try {
                    val success = runBinary(config, attemptId, bindAddress, onCodeRequired, inputProvider)
                    if (currentAttemptId.get() != attemptId) break
                    if (!success) {
                        LogRepository.e("Core execution failed or terminated unexpectedly.")
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    LogRepository.e("Execution cycle critical error: ${e.message}")
                }
                retryCount++
                if (connectionStatus.value == ConnectionStatus.RUNNING) {
                    retryCount = 1
                }
            }
        }
    }

    private suspend fun runBinary(config: AetherConfig, attemptId: Long, bindAddress: String, onCodeRequired: () -> Unit, inputProvider: suspend () -> String): Boolean = coroutineScope {
        val proc = PlatformProcess()
        try {
            val binaryPath = getBinaryManager(context).prepareBinary()
            val command = mutableListOf(binaryPath, "--bind", bindAddress)

            val httpBindHost = bindAddress.substringBefore(':')
            if (config.httpProxyEnabled) {
                command.add("--http-proxy")
                command.add("$httpBindHost:${config.httpPort}")
            }

            val routingFile = writeRoutingFile(config)
            if (routingFile != null) {
                command.add("--routes")
                command.add(routingFile.absolutePath)
            }

            if (config.protocol == AetherProtocol.ZERO_TRUST) {
                if (config.teamName.isNotEmpty()) {
                    command.add("--team")
                    command.add(config.teamName)
                }
                when {
                    config.accessToken.isNotEmpty() -> {
                        command.add("--access-token")
                        command.add(config.accessToken)
                    }
                    config.accessId.isNotEmpty() || config.accessSecret.isNotEmpty() -> {
                        command.add("--access-id")
                        command.add(config.accessId)
                        command.add("--access-secret")
                        command.add(config.accessSecret)
                    }
                    config.accessEmail.isNotEmpty() -> {
                        command.add("--access-email")
                        command.add(config.accessEmail)
                    }
                }
                if (config.useGateway) command.add("--gateway")
            }
            val effectiveIp = config.effectiveIpMode()
            command.add(when (effectiveIp) {
                AetherIpMode.IPV4 -> "-4"
                AetherIpMode.IPV6 -> "-6"
                else -> "--dual"
            })
            if (config.h2Mode) command.add("--h2")
            if (config.echEnabled) {
                command.add("--ech")
                command.add("auto")
            }
            if (config.h2Fragment) {
                command.add("--fragment")
                command.add("--fragment-size")
                command.add(config.fragmentSize)
                command.add("--fragment-delay")
                command.add(config.fragmentDelay)
            }
            if (config.noDataCheck) command.add("--no-data-check")
            if (config.quickReconnect) command.add("--quick-reconnect") else command.add("--no-quick-reconnect")
            val wiwOuter = if (config.protocol == AetherProtocol.GOOL) config.wiwOuter.trim() else ""
            val wiwInner = if (config.protocol == AetherProtocol.GOOL) config.wiwInner.trim() else ""
            val hasWiwManual = wiwOuter.isNotEmpty() || wiwInner.isNotEmpty()
            val effectivePeerForCmd = if (!hasWiwManual && (config.protocol == AetherProtocol.WG || config.protocol == AetherProtocol.GOOL) && config.wgPeer.isNotEmpty()) config.wgPeer else if (!hasWiwManual) config.peer else ""
            if (effectivePeerForCmd.isNotEmpty()) {
                command.add("--peer")
                command.add(effectivePeerForCmd)
            }
            if (!hasWiwManual && (config.protocol == AetherProtocol.WG || config.protocol == AetherProtocol.GOOL) && effectivePeerForCmd.isNotEmpty()) {
                command.add("--wg-peer")
                command.add(effectivePeerForCmd)
            }
            if (config.protocol == AetherProtocol.GOOL) {
                if (wiwOuter.isNotEmpty()) {
                    command.add("--wiw-outer")
                    command.add(wiwOuter)
                }
                if (wiwInner.isNotEmpty()) {
                    command.add("--wiw-inner")
                    command.add(wiwInner)
                }
                if (!hasWiwManual && config.wiwScan && effectivePeerForCmd.isEmpty()) {
                    command.add("--wiw-scan")
                }
            }
            if ((config.protocol == AetherProtocol.WG || config.protocol == AetherProtocol.GOOL)) {
                command.add("--keepalive")
                command.add(if (config.keepaliveEnabled) config.keepalive.toString() else "0")
            }
            if (config.tlsGroups.isNotEmpty()) {
                command.add("--tls-groups")
                command.add(config.tlsGroups)
            }
            command.add("--validate-secs")
            command.add(config.validateSecs.toString())
            command.add("--reconnect-secs")
            command.add(config.reconnectSecs.toString())
            if (config.noProfileRetry) command.add("--no-profile-retry")
            if (config.dnsEnabled && config.dnsList.isNotEmpty()) {
                command.add("--dns")
                command.add(config.dnsList)
            }
            if (config.upstreamProxyEnabled && config.upstreamProxy.isNotEmpty()) {
                command.add("--upstream")
                command.add(config.upstreamProxy)
                if (config.upstreamProxy.startsWith("http://", ignoreCase = true)) command.add("--h2")
            }

            if (config.torEnabled) {
                when (config.torMode) {
                    TorMode.TOR -> command.add("--tor")
                    TorMode.TOR_REVERSE -> command.add("--tor-reverse")
                    TorMode.TOR_ONLY -> command.add("--tor-only")
                }
                val torPort = io.github.immaghzbad.aetherst.core.TorController.activePort(config)
                command.add("--tor-bind")
                command.add("127.0.0.1:$torPort")
                val torDir = java.io.File(systemUtils.getFilesDir(), "tor")
                if (!torDir.exists()) torDir.mkdirs()
                command.add("--tor-dir")
                command.add(torDir.absolutePath)
                when (config.torBridgesMode.trim().lowercase()) {
                    "force" -> command.add("--tor-bridges")
                    "off" -> command.add("--no-tor-bridges")
                }
                config.torBridgeLines.split(";", "\n").map { it.trim() }.filter { it.isNotEmpty() }.forEach {
                    command.add("--tor-bridge")
                    command.add(it)
                }
                if (config.torPtDir.isNotBlank()) {
                    command.add("--tor-pt-dir")
                    command.add(config.torPtDir.trim())
                } else {
                    command.add("--tor-pt-dir")
                    command.add(torDir.absolutePath)
                }
                config.torPtBinaries.split(";", "\n").map { it.trim() }.filter { it.isNotEmpty() }.forEach {
                    command.add("--tor-pt")
                    command.add(it)
                }
            }
            val mimActive = config.protocol == AetherProtocol.MASQUE && config.mimEnabled
            val hasMimManual = config.mimOuter.isNotBlank() || config.mimInner.isNotBlank()
            if (mimActive) {
                command.add("--mim")
                if (config.mimOuter.isNotBlank()) {
                    command.add("--mim-outer")
                    command.add(config.mimOuter.trim())
                }
                if (config.mimInner.isNotBlank()) {
                    command.add("--mim-inner")
                    command.add(config.mimInner.trim())
                }
                if (config.mimScan && !hasMimManual) command.add("--mim-scan")
            }
            if (!config.quicV2Probe) command.add("--no-quic-v2")
            if (config.firewallMark.isNotBlank()) {
                command.add("--mark")
                command.add(config.firewallMark.trim())
            }

            val env = mutableMapOf<String, String>()
            env["AETHER_PROTOCOL"] = config.protocol.rawValue
            env["AETHER_NOIZE"] = config.noise.rawValue
            env["AETHER_SCAN"] = config.scanMode.rawValue
            env["AETHER_IP"] = config.effectiveIpMode().rawValue
            env["AETHER_SOCKS"] = bindAddress
            env["AETHER_LOG_LEVEL"] = config.coreLogLevel.rawValue
            env["AETHER_PERF_PROFILE"] = config.perfProfile.rawValue
            if (routingFile != null) {
                env["AETHER_ROUTES_FILE"] = routingFile.absolutePath
            }
            if (config.h2Mode) env["AETHER_MASQUE_HTTP2"] = "1"
            if (config.echEnabled) env["AETHER_ECH"] = "auto"
            val httpPort = config.httpPort.toIntOrNull() ?: 1820
            if (config.httpProxyEnabled) env["AETHER_HTTP_PROXY"] = "$httpBindHost:$httpPort"
            if (config.h2Fragment) {
                env["AETHER_MASQUE_H2_FRAGMENT"] = "1"
                env["AETHER_MASQUE_H2_FRAGMENT_SIZE"] = config.fragmentSize
                env["AETHER_MASQUE_H2_FRAGMENT_DELAY"] = config.fragmentDelay
            }
            if (config.noDataCheck) {
                env["AETHER_MASQUE_NO_DATA_CHECK"] = "1"
                env["AETHER_WG_NO_DATA_CHECK"] = "1"
            }
            if (config.quickReconnect) env["AETHER_QUICK_RECONNECT"] = "1" else env["AETHER_QUICK_RECONNECT"] = "0"
            if (config.protocol == AetherProtocol.WG || config.protocol == AetherProtocol.GOOL) {
                if (!hasWiwManual) {
                    if (config.wgPeer.isNotEmpty()) env["AETHER_WG_PEER"] = config.wgPeer else if (config.peer.isNotEmpty()) env["AETHER_WG_PEER"] = config.peer
                    if (config.peer.isNotEmpty()) env["AETHER_PEER"] = config.peer
                }
                if (config.protocol == AetherProtocol.GOOL) {
                    if (wiwOuter.isNotEmpty()) env["AETHER_WIW_OUTER_PEER"] = wiwOuter
                    if (wiwInner.isNotEmpty()) env["AETHER_WIW_INNER_PEER"] = wiwInner
                    if (!hasWiwManual && config.wiwScan && effectivePeerForCmd.isEmpty()) env["AETHER_WIW_PEERS"] = "auto"
                }
            } else {
                if (config.peer.isNotEmpty()) env["AETHER_PEER"] = config.peer
            }
            env["AETHER_WG_KEEPALIVE"] = if (config.keepaliveEnabled) config.keepalive.toString() else "0"
            env["AETHER_WG_ENDPOINT_COOLDOWN_SECS"] = config.wgEndpointCooldownSecs.toString()
            env["AETHER_MASQUE_VALIDATE_SECS"] = config.validateSecs.toString()
            env["AETHER_WG_VALIDATE_SECS"] = config.validateSecs.toString()
            env["AETHER_MASQUE_RECONNECT_SECS"] = config.reconnectSecs.toString()
            env["AETHER_WG_RECONNECT_SECS"] = config.reconnectSecs.toString()
            if (config.noProfileRetry) env["AETHER_WG_NO_PROFILE_RETRY"] = "1"
            if (config.tlsGroups.isNotEmpty()) env["AETHER_TLS_GROUPS"] = config.tlsGroups
            if (config.masqueMtu > 0) env["AETHER_MASQUE_MTU"] = config.masqueMtu.toString()
            if (config.netstackTcpRx > 0) env["AETHER_NETSTACK_TCP_RX"] = config.netstackTcpRx.toString()
            if (config.netstackTcpTx > 0) env["AETHER_NETSTACK_TCP_TX"] = config.netstackTcpTx.toString()
            if (config.protocol == AetherProtocol.ZERO_TRUST) {
                if (config.teamName.isNotEmpty()) env["AETHER_TEAM"] = config.teamName
                when {
                    config.accessToken.isNotEmpty() -> env["AETHER_ACCESS_TOKEN"] = config.accessToken
                    config.accessId.isNotEmpty() || config.accessSecret.isNotEmpty() -> {
                        env["AETHER_ACCESS_ID"] = config.accessId
                        env["AETHER_ACCESS_SECRET"] = config.accessSecret
                        env["AETHER_ACCESS_CLIENT_ID"] = config.accessId
                        env["AETHER_ACCESS_CLIENT_SECRET"] = config.accessSecret
                    }
                    config.accessEmail.isNotEmpty() -> env["AETHER_ACCESS_EMAIL"] = config.accessEmail
                }
                if (config.useGateway) env["AETHER_GATEWAY"] = "1"
            }
            if (config.dnsEnabled && config.dnsList.isNotEmpty()) env["AETHER_DNS"] = config.dnsList
            if (config.upstreamProxyEnabled && config.upstreamProxy.isNotEmpty()) env["AETHER_UPSTREAM"] = config.upstreamProxy
            if (config.torEnabled) {
                env["AETHER_TOR"] = when (config.torMode) {
                    TorMode.TOR -> "chain"
                    TorMode.TOR_REVERSE -> "reverse"
                    TorMode.TOR_ONLY -> "only"
                }
                val torPort = io.github.immaghzbad.aetherst.core.TorController.activePort(config)
                env["AETHER_TOR_BIND"] = "127.0.0.1:$torPort"
                val torDir = java.io.File(systemUtils.getFilesDir(), "tor")
                if (!torDir.exists()) torDir.mkdirs()
                env["AETHER_TOR_DIR"] = torDir.absolutePath
                when (config.torBridgesMode.trim().lowercase()) {
                    "force" -> env["AETHER_TOR_BRIDGES"] = "auto"
                    "off" -> env["AETHER_TOR_BRIDGES"] = "off"
                }
                val manualBridges = config.torBridgeLines.split(";", "\n").map { it.trim() }.filter { it.isNotEmpty() }
                if (manualBridges.isNotEmpty()) env["AETHER_TOR_BRIDGES"] = manualBridges.joinToString(";")
                val ptBinaries = config.torPtBinaries.split(";", "\n").map { it.trim() }.filter { it.isNotEmpty() }
                if (ptBinaries.isNotEmpty()) env["AETHER_TOR_PT"] = ptBinaries.joinToString(";")
                env["AETHER_TOR_PT_DIR"] = config.torPtDir.trim().ifEmpty { torDir.absolutePath }
                if (config.torCountry.isNotBlank()) env["AETHER_TOR_COUNTRY"] = config.torCountry.trim().lowercase()
            }
            if (mimActive) {
                env["AETHER_PROTOCOL"] = "mim"
                if (config.mimOuter.isNotBlank()) env["AETHER_MIM_OUTER_PEER"] = config.mimOuter.trim()
                if (config.mimInner.isNotBlank()) env["AETHER_MIM_INNER_PEER"] = config.mimInner.trim()
                if (config.mimScan && !hasMimManual) env["AETHER_MIM_PEERS"] = "auto"
            }
            if (!config.quicV2Probe) env["AETHER_QUIC_V2"] = "0"
            if (config.firewallMark.isNotBlank()) env["AETHER_MARK"] = config.firewallMark.trim()
            env["AETHER_ROUTE_SNIFF"] = if (config.routeSniffing) "1" else "0"
            env["AETHER_ROUTE_SNIFF_MS"] = config.sniffingTimeoutMs.toString()
            env["AETHER_REPROVISION"] = if (config.reprovision) "1" else "0"

            LogRepository.d("Executing: ${command.joinToString(" ")}")
            if (!proc.start(command, systemUtils.getFilesDir(), env)) {
                LogRepository.e("Failed to start process: $binaryPath. Check if file exists and is executable.")
                return@coroutineScope false
            }
            process = proc
            val inputJob = launch {
                while (isActive) {
                    val text = inputProvider()
                    if (text.isNotEmpty()) {
                        proc.writeLine(text)
                        LogRepository.d("Sent input to binary")
                    }
                }
            }
            while (isActive && currentAttemptId.get() == attemptId) {
                val line = proc.readLine() ?: break
                parseOutputLine(line, attemptId, config.protocol, onCodeRequired)
            }
            inputJob.cancel()
            val exitCode = proc.waitFor()
            LogRepository.i("Core process terminated (Exit code: $exitCode)")
            exitCode == 0
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            if (currentAttemptId.get() == attemptId) {
                LogRepository.e("Binary execution runtime error: ${e.message}")
            }
            false
        } finally {
            proc.destroy()
        }
    }

    private fun parseOutputLine(line: String, attemptId: Long, protocol: AetherProtocol, onCodeRequired: () -> Unit) {
        if (currentAttemptId.get() != attemptId) return
        val lower = line.lowercase()
        val source = if (lower.contains("tor")) "Tor" else "AetherCore"
        when {
            lower.contains(" error ") || lower.contains("[error]") -> LogRepository.e(line, source)
            lower.contains(" warn ") || lower.contains("[warn]") -> LogRepository.w(line, source)
            else -> LogRepository.i(line, source)
        }
        if (isZeroTrustCodePrompt(lower)) {
            onCodeRequired()
            return
        }
        if (lower.contains("tor")) {
            val bootstrapMatch = Regex("""bootstrapp?ed?\s+(\d{1,3})\s*%""").find(lower)
            if (bootstrapMatch != null) {
                bootstrapMatch.groupValues.getOrNull(1)?.toIntOrNull()?.let { io.github.immaghzbad.aetherst.core.TorController.notifyBootstrap(it) }
            }
            if (lower.contains("tor is ready") || lower.contains("leaves through tor") || lower.contains("way out")) {
                io.github.immaghzbad.aetherst.core.TorController.notifyCoreReady()
                io.github.immaghzbad.aetherst.core.TorController.notifyBootstrap(100)
                val readyPort = Regex("""127\.0\.0\.1:(\d+)""").find(line)?.groupValues?.getOrNull(1)?.toIntOrNull()
                io.github.immaghzbad.aetherst.core.TorController.notifySocksListening(readyPort ?: io.github.immaghzbad.aetherst.core.TorController.currentPort())
                io.github.immaghzbad.aetherst.core.TorController.notifyProxyReady(readyPort ?: io.github.immaghzbad.aetherst.core.TorController.currentPort())
            } else if (lower.contains("tor socks5 listening")) {
                val listenPort = Regex("""127\.0\.0\.1:(\d+)""").find(line)?.groupValues?.getOrNull(1)?.toIntOrNull()
                io.github.immaghzbad.aetherst.core.TorController.notifySocksListening(listenPort ?: io.github.immaghzbad.aetherst.core.TorController.currentPort())
            }
        }
        when {
            lower.contains("scanning") -> {
                goolOuterValidated = false
                updateState(ConnectionStatus.STARTING, attemptId)
            }
            lower.contains("validating") -> updateState(ConnectionStatus.VALIDATING, attemptId)
            (protocol == AetherProtocol.MASQUE && (lower.contains("tunnel validated") || lower.contains("connect-ip status: 200"))) ||
            (protocol == AetherProtocol.WG && (lower.contains("wireguard tunnel validated") || lower.contains("handshake complete"))) -> {
                updateState(ConnectionStatus.RUNNING, attemptId)
            }
            protocol == AetherProtocol.GOOL -> {
                if (lower.contains("outer") && lower.contains("tunnel validated")) goolOuterValidated = true
                if (lower.contains("inner") && lower.contains("tunnel validated") && goolOuterValidated) {
                    updateState(ConnectionStatus.RUNNING, attemptId)
                }
            }
            lower.contains("reconnecting") || lower.contains("tunnel lost") || lower.contains("handshake timeout") ||
                lower.contains("handshake failed") || lower.contains("connection refused") || lower.contains("all gateways failed") -> {
                goolOuterValidated = false
                updateState(ConnectionStatus.RECONNECTING, attemptId)
            }
        }
    }

    private fun isZeroTrustCodePrompt(line: String): Boolean {
        return line.contains("code") && (
            line.contains("enter") || line.contains("login") || line.contains("verif") ||
            line.contains("confirm") || line.contains("otp") || line.contains("one-time") ||
            line.contains("type the") || line.contains("paste") || line.contains("prompt")
        )
    }

    private fun updateState(state: ConnectionStatus, attemptId: Long) {
        if (currentAttemptId.get() == attemptId) _connectionStatus.value = state
    }

    private fun writeRoutingFile(config: AetherConfig): java.io.File? {
        val block = config.routingRules.filter { it.mode == RoutingMode.BLOCK }
        if (block.isEmpty()) return null

        return try {
            val file = java.io.File(systemUtils.getFilesDir(), "routing.ast")
            val content = StringBuilder()
            content.append("[block]\n")
            block.forEach { content.append(formatRoutingPattern(it.pattern)).append("\n") }
            content.append("\n")
            file.writeText(content.toString())
            file
        } catch (e: Exception) {
            LogRepository.e("Failed to write routing file: ${e.message}")
            null
        }
    }

    private fun formatRoutingPattern(pattern: String): String {
        val trimmed = pattern.trim()
        if (trimmed.startsWith("domain:") || trimmed.startsWith("ip:") ||
            trimmed.startsWith("keyword:") || trimmed.startsWith("regexp:") ||
            trimmed == "private") {
            return trimmed
        }

        val isIp = trimmed.all { it.isDigit() || it == '.' || it == ':' || it == '/' || (it.lowercaseChar() in 'a'..'f') } &&
                (trimmed.contains('.') || trimmed.contains(':'))

        return if (isIp) "ip:$trimmed" else "domain:$trimmed"
    }

    fun stop() {
        currentAttemptId.incrementAndGet()
        _connectionStatus.value = ConnectionStatus.STOPPED
        runnerJob?.cancel()
        process?.destroy()
        process = null
        LogRepository.i("System core shutdown initiated.")
    }

    fun release() {
        stop()
        scope.cancel()
    }
}
