import React, { useState } from "react";
import { FixedSizeList as List } from "react-window";

export default function AlertTable({ alerts, selectedAlerts, setSelectedAlerts }) {
  const [lastSelectedIndex, setLastSelectedIndex] = useState(null);
  const [filterClass, setFilterClass] = useState("all");
  const [filterId, setFilterId] = useState("all");
  const [filterTimestamp, setFilterTimestamp] = useState("all");
  const [searchText, setSearchText] = useState("");

  if (!alerts || alerts.length === 0) {
    return <div className="text-gray-300">📭 Nessun dato caricato.</div>;
  }

  const formatTimestamp = (unixTimestamp) => {
    const date = new Date(unixTimestamp * 1000);
    return date.toLocaleString("it-IT", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const allTimestamps = [...new Set(alerts.map((a) => formatTimestamp(a.timestamp)))];
  const allIds = [...new Set(alerts.map((a) => a.id))];

  const filteredAlerts = alerts.slice().sort((a, b) => a.id - b.id).filter((alert) => {//= alerts.filter((alert) => {
    const tsFormatted = formatTimestamp(alert.timestamp);
    const textMatch =
      alert.explanation.toLowerCase().includes(searchText.toLowerCase()) ||
      String(alert.id).includes(searchText) ||
      String(alert.timestamp).includes(searchText) ||
      tsFormatted.toLowerCase().includes(searchText.toLowerCase());

    const matchClass = filterClass === "all" || alert.class === filterClass;
    const matchId = filterId === "all" || alert.id === Number(filterId);
    const matchTimestamp = filterTimestamp === "all" || tsFormatted === filterTimestamp;

    return matchClass && matchId && matchTimestamp && textMatch;
  });

  const handleRowClick = (alert, index, event) => {
    if (event.shiftKey && lastSelectedIndex !== null) {
      const start = Math.min(index, lastSelectedIndex);
      const end = Math.max(index, lastSelectedIndex);
      const newSelection = filteredAlerts.slice(start, end + 1);
      const combined = [...selectedAlerts];

      newSelection.forEach((a) => {
        if (!combined.find((item) => item.id === a.id)) {
          combined.push(a);
        }
      });
      setSelectedAlerts(combined);
    } else if (event.ctrlKey || event.metaKey) {
      const exists = selectedAlerts.find((a) => a.id === alert.id);
      if (exists) {
        setSelectedAlerts(selectedAlerts.filter((a) => a.id !== alert.id));
      } else {
        setSelectedAlerts([...selectedAlerts, alert]);
      }
    } else {
      setSelectedAlerts([alert]);
    }
    setLastSelectedIndex(index);
  };

  const handleExportJSON = (data) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "risultati_filtrati.json";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const resetFilters = () => {
    setFilterClass("all");
    setFilterId("all");
    setFilterTimestamp("all");
    setSearchText("");
  };

  const handleExportCSV = (data) => {
    const header = "ID;Timestamp;Classificazione;Spiegazione";
    const rows = data.map((alert) =>
      `${alert.id};${formatTimestamp(alert.timestamp)};${alert.class.replace(
        "_",
        " "
      )};"${alert.explanation.replace(/"/g, '""')}"`
    );
    const csvContent = [header, ...rows].join("\n");
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "risultati_filtrati.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const Row = ({ index, style }) => {
    const alert = filteredAlerts[index];
    const isSelected = selectedAlerts.some((a) => a.id === alert.id);

    return (
      <div
        style={style}
        key={alert.id}
        onClick={(e) => handleRowClick(alert, index, e)}
        className={`grid grid-cols-[100px_180px_160px_1fr] border-t border-[#1F4D45] cursor-pointer hover:bg-[#003C35] ${
          isSelected ? "bg-[#00FFB3] text-black font-bold" : "text-white"
        }`}
      >
        <div className="p-2 w-[100px] truncate">{alert.id}</div>
        <div className="p-2 w-[180px] truncate">{formatTimestamp(alert.timestamp)}</div>
        <div
          className={`p-2 w-[160px] font-semibold ${
            alert.class === "real_threat"
              ? "text-red-400"
              : alert.class === "false_positive"
              ? "text-green-400"
              : "text-yellow-300"
          }`}
        >
          {alert.class.replace("_", " ")}
        </div>
        <div className="p-2 whitespace-normal break-words">{alert.explanation}</div>
      </div>
    );
  };

  return (
    <div className="p-4 border border-[#1F4D45] rounded-xl shadow bg-[#0A2E2A] text-white">
      <h2 className="text-lg font-bold mb-4 text-[#26FF8A]">Risultati Classificazione</h2>

      {/* FILTRI */}
      <div className="flex flex-wrap gap-4 mb-4">
        <div>
          <label className="mr-2 font-semibold text-[#8FD9C2]">Classificazione:</label>
          <select
            className="bg-white text-black border p-1 rounded"
            value={filterClass}
            onChange={(e) => setFilterClass(e.target.value)}
          >
            <option value="all">Tutte</option>
            <option value="real_threat">Real Threat</option>
            <option value="false_positive">False Positive</option>
            <option value="undetermined">Undetermined</option>
          </select>
        </div>

        <div>
          <label className="mr-2 font-semibold text-[#8FD9C2]">ID:</label>
          <select
            className="bg-white text-black border p-1 rounded"
            value={filterId}
            onChange={(e) => setFilterId(e.target.value)}
          >
            <option value="all">Tutti</option>
            {allIds.map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
          </select>
        </div>

        <div>
          <label className="mr-2 font-semibold text-[#8FD9C2]">Timestamp:</label>
          <select
            className="bg-white text-black border p-1 rounded"
            value={filterTimestamp}
            onChange={(e) => setFilterTimestamp(e.target.value)}
          >
            <option value="all">Tutti</option>
            {allTimestamps.map((ts) => (
              <option key={ts} value={ts}>{ts}</option>
            ))}
          </select>
        </div>

        <div>
          <label className="mr-2 font-semibold text-[#8FD9C2]">Ricerca:</label>
          <input
            type="text"
            placeholder="Cerca testo libero..."
            className="bg-white text-black border p-1 rounded placeholder-black"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
          />
        </div>
      </div>

      {/* BOTTONI */}
      <div className="flex flex-wrap gap-3 mb-4">
        <button onClick={() => handleExportJSON(filteredAlerts)} className="bg-[#26FF8A] hover:bg-[#00e07a] text-[#00352F] font-semibold py-1 px-3 rounded">Esporta JSON</button>
        <button onClick={() => handleExportCSV(filteredAlerts)} className="bg-[#00B383] hover:bg-[#009e72] text-[#00352F] font-semibold py-1 px-3 rounded">Esporta CSV</button>
        <button onClick={resetFilters} className="bg-[#1F4D45] hover:bg-[#173e38] text-white font-semibold py-1 px-3 rounded">Reset filtri</button>
      </div>

      {/* TABELLA */}
      <div className="overflow-x-auto">
        {/* Header */}
        <div className="grid grid-cols-[100px_180px_160px_1fr] bg-[#001F1D] text-[#8FD9C2] font-semibold text-sm">
          <div className="p-2 w-[100px]">ID</div>
          <div className="p-2 w-[180px]">Timestamp</div>
          <div className="p-2 w-[160px]">Classificazione</div>
          <div className="p-2">Spiegazione</div>
        </div>

        {/* Riga virtualizzata */}
        <List
          height={500}
          itemCount={filteredAlerts.length}
          itemSize={70}
          width={"100%"}
        >
          {Row}
        </List>
      </div>

      {filteredAlerts.length === 0 && (
        <div className="text-[#8FD9C2] mt-4">Nessun risultato trovato.</div>
      )}
    </div>
  );
}
