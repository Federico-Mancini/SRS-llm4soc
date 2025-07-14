import React, { useRef, useState } from "react";
import {
  uploadFileToAPI,
  analyzeAlertsOnServer,
  fetchResultsFromAPI
} from "../services/apiService";

export default function UploadFile({
  onDataLoaded,
  setLoading,
  setError,
  setElapsedSeconds,
  setAttemptCount,
  setSelectedAlert,
  setChatResetKey
}) {
  // ⌛ Intervallo tra i tentativi di richiesta risultati (in millisecondi)
  const DELAY_MS = 15000;

  const [lastFile, setLastFile] = useState(null);
  const [interrupted, setInterrupted] = useState(false);
  const timerRef = useRef(null);
  const fileInputRef = useRef(null);

  const startTimer = () => {
    setElapsedSeconds(0);
    timerRef.current = setInterval(() => {
      setElapsedSeconds((prev) => {
        const next = prev + 1;
        setElapsedSeconds(next);
        return next;
      });
    }, 1000);
  };

  const stopTimer = () => clearInterval(timerRef.current);

  const processFile = async (file) => {
    setLoading(true);
    setError("");
    setAttemptCount(0);
    setInterrupted(false);
    startTimer();

    try {
      await uploadFileToAPI(file);
      await analyzeAlertsOnServer(file.name);

      let result = null;

      while (true) {
        if (interrupted) {
          throw new Error("esecuzione interrotta manualmente");
        }

        try {
          const res = await fetchResultsFromAPI(file.name);
          result = res.results || res;
          break;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, DELAY_MS));
          setAttemptCount((prev) => {
            const next = prev + 1;
            setAttemptCount(next);
            return next;
          });
        }
      }

      if (result) {
        onDataLoaded(result);
        if (setSelectedAlert) setSelectedAlert(null);
        if (setChatResetKey) setChatResetKey(prev => prev + 1);
      }
    } catch (err) {
      setError("❌ Errore: " + err.message);
    } finally {
      stopTimer();
      setLoading(false);
    }
  };

  const handleFileChange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setLastFile(file);
    await processFile(file);
  };

  const handleReloadClick = async () => {
    if (lastFile) {
      await processFile(lastFile);
    }
  };

  const handleStopClick = () => {
    setInterrupted(true);
    stopTimer();
    setError("❌ Errore: esecuzione interrotta manualmente");
    setLoading(false);
  };

  return (
    <div className="flex items-center gap-3">
      {/* 🔄 Pulsante Ricarica */}
      <button
        onClick={handleReloadClick}
        disabled={!lastFile}
        className={`text-2xl transition-all ${
          lastFile
            ? "text-[#26FF8A] hover:text-[#00e07a]"
            : "text-gray-500 opacity-50 cursor-not-allowed"
        }`}
        title="Ricarica file"
      >
        🔄
      </button>

      {/* ⛔ Pulsante Stop */}
      <button
        onClick={handleStopClick}
        disabled={!lastFile}
        className={`text-2xl transition-all ${
          lastFile
            ? "text-red-400 hover:text-red-600"
            : "text-gray-500 opacity-50 cursor-not-allowed"
        }`}
        title="Interrompi esecuzione"
      >
        ⛔
      </button>

      {/* 📁 Input */}
      <div className="flex flex-col">
        <input
          ref={fileInputRef}
          type="file"
          accept=".json,.csv,.jsonl"
          className="bg-[#002922] text-[#8FD9C2] border border-[#8FD9C2] p-2 rounded"
          onChange={handleFileChange}
        />
      </div>
    </div>
  );
}