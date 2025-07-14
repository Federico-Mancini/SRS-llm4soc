const BASE_URL = "http://34.77.90.106:8000";

export async function uploadFileToAPI(file) {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(`${BASE_URL}/upload-dataset`, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) throw new Error("Errore upload file");
  return response.json();
}

export async function analyzeAlertsOnServer(datasetFilename) {
  const response = await fetch(`${BASE_URL}/analyze-dataset?dataset_filename=${datasetFilename}`, {
    method: "GET",
  });

  if (!response.ok) throw new Error("Errore analisi alert");
  return response.json();
}

export async function fetchResultsFromAPI(datasetFilename) {
  const response = await fetch(`${BASE_URL}/result?dataset_filename=${datasetFilename}`);

  if (!response.ok) throw new Error("Errore fetch risultati");
  return response.json();
}

export async function sendMessageToChatAPI(alertList, questionText) {
  const alertsPayload = Array.isArray(alertList) ? alertList : [alertList];

  const response = await fetch(`${BASE_URL}/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      alerts: alertsPayload,
      question: questionText
    }),
  });

  if (!response.ok) throw new Error("Errore richiesta chatbot");

  const data = await response.json();
  return { reply: data.explanation };
}
