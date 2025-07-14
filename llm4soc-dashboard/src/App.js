import React, { useState } from "react";
import UploadFile from "./components/UploadFile";
import AlertTable from "./components/AlertTable";
import Chatbot from "./components/Chatbot";

function App() {
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [attemptCount, setAttemptCount] = useState(0);

  const [selectedAlerts, setSelectedAlerts] = useState([]);
  const [chatResetKey, setChatResetKey] = useState(0);
  //const [datasetFilename, setDatasetFilename] = useState("");

  const handleDataLoaded = (data) => {
    setAlerts(data);
    setSelectedAlerts([]);
    setChatResetKey((prev) => prev + 1);
  };

  return (
    <div className="min-h-screen bg-[#00352F] text-[#E6FFF5]">
      {/* HEADER */}
      <div className="bg-[#001F1D] p-4">
        <h1 className="text-2xl font-bold text-[#26FF8A]">
          Dashboard - Analisi alert
        </h1>
      </div>

      <div className="p-3">
        {/* UPLOAD FILE */}
        <div className="p-1.5 mb-6 h-[60px] w-full bg-[#0A2E2A] border border-[#1F4D45] rounded-xl shadow">
          <div className="flex items-center justify-start gap-4 h-full">
            <h2 className="text-base font-bold text-[#E6FFF5] whitespace-nowrap">
              📄 Carica file .csv/.json
            </h2>
            <UploadFile
              onDataLoaded={handleDataLoaded}
              setLoading={setLoading}
              setError={setError}
              setElapsedSeconds={setElapsedSeconds}
              setAttemptCount={setAttemptCount}
            />
          </div>
        </div>

        {/* STATO */}
        {loading && (
          <div className="text-[#8FD9C2] flex items-center gap-4 mt-2 ml-2 text-sm">
            <span>⏳ Caricamento dati...</span>
            <span>⏱️ {elapsedSeconds} s</span>
            <span>🔁 Tentativi: {attemptCount}</span>
          </div>
        )}

        {/* ERRORE */}
        {error && <p className="text-red-400">❌ Errore: {error}</p>}

        {/* CHATBOT */}
        {!loading && !error && alerts.length > 0 && (
          <Chatbot
            key={chatResetKey}
            selectedAlerts={selectedAlerts}
          />
        )}

        {/* TABELLA */}
        {!loading && !error && (
          <AlertTable
            alerts={alerts}
            selectedAlerts={selectedAlerts}
            setSelectedAlerts={setSelectedAlerts}
          />
        )}
      </div>
    </div>
  );
}

export default App;
