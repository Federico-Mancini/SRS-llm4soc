import React, { useState } from "react";
import { sendMessageToChatAPI } from "../services/apiService";

export default function Chatbot({ selectedAlerts }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);

  const hasSelection = selectedAlerts && selectedAlerts.length > 0;

  const handleSend = async () => {
    if (!input.trim() || !hasSelection) return;

    const userMessage = { sender: "user", text: input };
    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setLoading(true);

    console.log("Invio al chatbot:", selectedAlerts);

    try {
      const response = await sendMessageToChatAPI(
        selectedAlerts,
        input
      );
      console.log("Risposta dal backend:", response);
      const botMessage = { sender: "bot", text: response.reply };
      setMessages((prev) => [...prev, botMessage]);
      console.log("Messaggio ricevuto dal chatbot:", response.reply);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { sender: "bot", text: "❌ Errore nella risposta del chatbot." },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter") handleSend();
  };

  return (
    <div className="p-4 bg-[#0A2E2A] border border-[#1F4D45] rounded-xl shadow text-white mt-1">
      <h2 className="text-base font-bold mb-4 text-[#26FF8A]">💬 Chatbot LLM</h2>

      {/* Messaggi */}
      <div
        className={`overflow-y-auto p-3 mb-2 bg-[#002922] rounded transition-all duration-300 ${
          isExpanded ? "h-96" : "h-20"
        }`}
      >
        {messages.map((msg, index) => (
          <div
            key={index}
            className={`mb-2 ${msg.sender === "user" ? "text-right" : "text-left"}`}
          >
            <div
              className={`inline-block px-1 py-0 rounded-md break-words max-w-[50%] ${
                msg.sender === "user"
                  ? "bg-[#26FF8A] text-black"
                  : "bg-[#1F4D45] text-white"
              }`}
            >
              {msg.text}
            </div>
          </div>
        ))}
        {loading && (
          <div className="text-[#8FD9C2] text-sm italic">
            ⏳ Sto generando la risposta...
          </div>
        )}
      </div>

      {/* Bottone espandi */}
      <div className="flex justify-end mb-2">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-sm text-[#8FD9C2] hover:text-white"
        >
          {isExpanded ? "🔼 Riduci conversazione" : "🔽 Espandi conversazione"}
        </button>
      </div>

      {/* Input e invio */}
      <div className="flex gap-2">
        <input
          type="text"
          className="flex-grow p-2 rounded bg-white text-black placeholder-black"
          placeholder={
            hasSelection
              ? `Scrivi la tua domanda (${selectedAlerts.length} alert selezionati)...`
              : "⚠️ Seleziona uno o più alert"
          }
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={!hasSelection}
        />
        <button
          onClick={handleSend}
          disabled={!hasSelection}
          className={`font-bold py-2 px-4 rounded ${
            hasSelection
              ? "bg-[#26FF8A] hover:bg-[#00e07a] text-[#00352F]"
              : "bg-gray-400 text-white cursor-not-allowed"
          }`}
        >
          Invia
        </button>
      </div>
    </div>
  );
}
