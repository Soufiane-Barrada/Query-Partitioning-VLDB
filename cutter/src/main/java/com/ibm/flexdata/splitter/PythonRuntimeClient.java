package com.ibm.flexdata.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.flexdata.splitter.OperatorFeaturesCollector.OperatorFeatures;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PythonRuntimeClient {

    private final String pythonExecutable;
    private final String scriptPath;
    private final Map<Engine, Map<OperatorKind, String>> modelPaths;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<Engine, Map<OperatorKind, Worker>> workers = new ConcurrentHashMap<>();

    public PythonRuntimeClient(String pythonExecutable,
                               String scriptPath,
                               Map<Engine, Map<OperatorKind, String>> modelPaths) {
        this.pythonExecutable = pythonExecutable;
        this.scriptPath = scriptPath;
        this.modelPaths = modelPaths;
    }

    private String getModelPath(Engine engine, OperatorKind kind) {
        Map<OperatorKind, String> byKind = modelPaths.get(engine);
        if (byKind == null) return null;
        return byKind.get(kind);
    }

    private Worker getOrCreateWorker(Engine engine, OperatorKind kind, String modelPath) throws Exception {
        Map<OperatorKind, Worker> byKind =
            workers.computeIfAbsent(engine, e -> new ConcurrentHashMap<>());
        Worker existing = byKind.get(kind);
        if (existing != null) {
            return existing;
        }
        Worker created = new Worker(modelPath);
        byKind.put(kind, created);
        return created;
    }

    private final class Worker {
        private final Process process;
        private final OutputStream stdin;
        private final BufferedReader stdout;

        Worker(String modelPath) throws Exception {
            ProcessBuilder pb = new ProcessBuilder(
                pythonExecutable,
                scriptPath,
                "--run-path",
                modelPath,
                "--server"
            );
            this.process = pb.start();
            this.stdin = process.getOutputStream();
            this.stdout = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));

            // Drain stderr in background so the process does not block on a full buffer.
            Thread stderrDrainer = new Thread(() -> {
                try (InputStream es = process.getErrorStream()) {
                    byte[] buf = new byte[4096];
                    while (es.read(buf) != -1) {
                        // Discard or route elsewhere if needed.
                    }
                } catch (Exception ignored) {
                }
            }, "PythonRuntimeClient-stderr-" + modelPath);
            stderrDrainer.setDaemon(true);
            stderrDrainer.start();
        }

        synchronized double predict(Map<String, Object> featMap, Engine engine, OperatorKind kind) {
            try {
                // Write one JSON object per line without closing the shared stream
                String json = mapper.writeValueAsString(featMap);
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                stdin.write(bytes);
                stdin.write('\n');
                stdin.flush();

                String line = stdout.readLine();
                if (line == null || line.trim().isEmpty()) {
                    System.err.println("[PythonRuntimeClient] Empty stdout line from Python for "
                            + engine + "/" + kind);
                    return 0.0;
                }

                Map<?, ?> response;
                try {
                    response = mapper.readValue(line, Map.class);
                } catch (Exception parse) {
                    System.err.println("[PythonRuntimeClient] Failed to parse JSON from Python for "
                            + engine + "/" + kind + ": " + parse.getMessage());
                    System.err.println("[PythonRuntimeClient] Raw line was:\n" + line);
                    return 0.0;
                }

                if (response.containsKey("error")) {
                    Object err = response.get("error");
                    System.err.println("[PythonRuntimeClient] Python error for "
                            + engine + "/" + kind + ": " + err);
                    return 0.0;
                }

                Object t = response.get("time");
                Object uObj = response.get("unit");
                String u = (uObj instanceof String) ? (String) uObj : null;

                double ret;
                if (t instanceof Number) {
                    ret = ((Number) t).doubleValue();
                } else if (t != null) {
                    ret = Double.parseDouble(t.toString());
                } else {
                    System.err.println("[PythonRuntimeClient] Response JSON missing 'time' for "
                            + engine + "/" + kind + ": " + line);
                    return 0.0;
                }

                if ("s".equals(u)) {
                    ret = ret * 1000.0;
                }
                return ret;
            } catch (Exception e) {
                System.err.println("[PythonRuntimeClient] Error predicting for "
                        + engine + "/" + kind + ": " + e.getMessage());
                e.printStackTrace(System.err);
                return 0.0;
            }
        }
    }


    public double predict(Engine engine, OperatorKind kind, OperatorFeatures features) {
        Map<String, Object> featMap = FeatureVectorBuilder.build(kind, features);
        if (featMap == null) {
            return 0.0;
        }

        String modelPath = getModelPath(engine, kind);
        if (modelPath == null || modelPath.isEmpty()) {
            // No model configured for this engine/kind
            if (kind != OperatorKind.OTHER) {
                System.err.println("[PythonRuntimeClient] No model configured for "
                        + engine + "/" + kind);
            }
            return 0.0;
        }

        try {
            Worker w = getOrCreateWorker(engine, kind, modelPath);
            return w.predict(featMap, engine, kind);
        } catch (Exception e) {
            System.err.println("[PythonRuntimeClient] Error predicting for "
                    + engine + "/" + kind + ": " + e.getMessage());
            e.printStackTrace(System.err);
            return 0.0;
        }
    }

}
 