document.addEventListener("DOMContentLoaded", () => {
  const queryInputTextarea = document.getElementById("query-input");
  const submitButton = document.getElementById("submit-button");
  const loadingSpinner = document.getElementById("loading-spinner");

  const statusMessage = document.getElementById("status-message");
  const errorOutput = document.getElementById("error-output");
  const resultTableContainer = document.getElementById(
    "result-table-container"
  );
  const structureArea = document.getElementById("structure-area");
  const structureTableName = document.getElementById("structure-table-name");
  const structureColumnList = document.getElementById("structure-column-list");

  const tableList = document.getElementById("table-list");

  const historyButton = document.getElementById("history-button");
  const historyList = document.getElementById("query-history-list");
  let queryHistory = JSON.parse(localStorage.getItem("sqlrQueryHistory")) || [];

  const editor = CodeMirror.fromTextArea(queryInputTextarea, {
    mode: "text/x-sql",
    theme: "base16-light",
    lineNumbers: true,
    lineWrapping: true,
    indentWithTabs: true,
    smartIndent: true,
    matchBrackets: true,
    autofocus: true,
  });
  setTimeout(() => editor.refresh(), 1);

  async function executeQuery(query, isInternal = false) {
    if (!query) {
      if (!isInternal) {
        displayStatus("Please enter a query.", "warning");
      }
      return null;
    }

    setLoading(true);
    if (!isInternal) {
      displayStatus("Executing...", "info");
      addToHistory(query);
      clearResults();
    }

    try {
      const response = await fetch("/api/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query: query }),
      });

      const data = await response.json();

      if (!isInternal) {
        handleApiResponse(data);
      }
      return data;
    } catch (error) {
      console.error("Error executing query:", error);
      if (!isInternal) {
        displayError(`Network or fetch error: ${error.message}`);
        displayStatus("Error occurred.", "danger");
      }
      return null;
    } finally {
      setLoading(false);
    }
  }

  function setLoading(isLoading) {
    submitButton.disabled = isLoading;
    if (isLoading) {
      loadingSpinner.classList.remove("hidden");
    } else {
      loadingSpinner.classList.add("hidden");
    }
  }

  function displayStatus(message, type = "info") {
    statusMessage.textContent = message;
    statusMessage.className = "p-3 mb-3 text-sm rounded border";
    switch (type) {
      case "success":
        statusMessage.classList.add(
          "bg-green-100",
          "border-green-200",
          "text-green-700"
        );
        break;
      case "warning":
        statusMessage.classList.add(
          "bg-yellow-100",
          "border-yellow-200",
          "text-yellow-700"
        );
        break;
      case "danger":
        statusMessage.classList.add(
          "bg-red-100",
          "border-red-200",
          "text-red-700"
        );
        break;
      case "info":
      default:
        statusMessage.classList.add(
          "bg-blue-100",
          "border-blue-200",
          "text-blue-700"
        );
        break;
      case "secondary":
        statusMessage.classList.add(
          "bg-gray-100",
          "border-gray-200",
          "text-gray-700"
        );
        break;
    }
  }

  function displayError(errorDetails) {
    if (typeof errorDetails === "string") {
      errorOutput.textContent = errorDetails;
    } else {
      errorOutput.textContent = JSON.stringify(errorDetails, null, 2);
    }
    errorOutput.classList.remove("hidden");
    resultTableContainer.innerHTML = "";
    structureArea.classList.add("hidden");
  }

  function displayTable(result) {
    const table = document.createElement("table");
    table.className =
      "min-w-full divide-y divide-gray-200 border border-gray-200";
    const thead = table.createTHead();
    thead.className = "bg-gray-50";
    const tbody = table.createTBody();
    tbody.className = "bg-white divide-y divide-gray-200";

    const headerRow = thead.insertRow();
    result.columns.forEach((colName) => {
      const th = document.createElement("th");
      th.scope = "col";
      th.className =
        "px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider";
      th.textContent = colName;
      headerRow.appendChild(th);
    });

    if (result.rows.length === 0) {
      const row = tbody.insertRow();
      const cell = row.insertCell();
      cell.colSpan = result.columns.length;
      cell.className = "px-3 py-2 text-sm text-gray-500 italic text-center";
      cell.textContent = "(No rows returned)";
    } else {
      result.rows.forEach((rowData) => {
        const row = tbody.insertRow();
        rowData.forEach((cellData) => {
          const cell = row.insertCell();
          cell.className = "px-3 py-2 text-sm text-gray-700 whitespace-nowrap";
          if (cellData === null || cellData === undefined) {
            cell.textContent = "NULL";
            cell.classList.add("text-gray-400", "italic");
          } else if (typeof cellData === "object") {
            cell.textContent = JSON.stringify(cellData);
          } else {
            cell.textContent = cellData;
          }
        });
      });
    }

    resultTableContainer.innerHTML = "";
    resultTableContainer.appendChild(table);
    errorOutput.classList.add("hidden");
    structureArea.classList.add("hidden");
  }

  function displayStructure(tableName, columns) {
    structureTableName.textContent = tableName;
    structureColumnList.innerHTML = "";
    if (columns && columns.length > 0) {
      columns.forEach((colInfo) => {
        const colName = typeof colInfo === "string" ? colInfo : colInfo.name;
        const colType = typeof colInfo === "string" ? "" : colInfo.type;

        const li = document.createElement("li");
        li.className = "flex justify-between items-center px-2 py-1 text-sm";
        const nameSpan = document.createElement("span");
        nameSpan.className = "font-mono text-gray-700";
        nameSpan.textContent = colName;
        li.appendChild(nameSpan);
        if (colType) {
          const typeSpan = document.createElement("span");
          typeSpan.className =
            "font-mono text-xs text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded";
          typeSpan.textContent = colType;
          li.appendChild(typeSpan);
        }
        structureColumnList.appendChild(li);
      });
    } else {
      const li = document.createElement("li");
      li.className = "px-2 py-1 text-sm text-gray-500 italic";
      li.textContent = "Could not determine columns (or table is empty).";
      structureColumnList.appendChild(li);
    }
    structureArea.classList.remove("hidden");
    resultTableContainer.innerHTML = "";
    errorOutput.classList.add("hidden");
  }

  function handleApiResponse(data) {
    if (data.error) {
      displayError(data.details || "Unknown error");
      displayStatus(data.message || "Error occurred", "danger");
    } else if (data.result) {
      if (data.result.RowSet) {
        const rowSet = data.result.RowSet;
        displayTable(rowSet);
        displayStatus(
          `${data.message || "Execution successful"}. (${
            rowSet.rows.length
          } row(s) returned)`,
          "success"
        );
      } else if (data.result.Msg) {
        clearResults();
        displayStatus(
          `${data.message || "Execution successful"}. ${data.result.Msg}`,
          "success"
        );
      } else {
        clearResults();
        displayStatus(
          `Execution finished with unexpected result type.`,
          "warning"
        );
        console.warn("Unexpected result structure:", data.result);
      }
    } else {
      clearResults();
      displayStatus(
        data.message || "Execution finished, no specific result.",
        "success"
      );
    }
  }

  function clearResults() {
    resultTableContainer.innerHTML = "";
    errorOutput.classList.add("hidden");
    errorOutput.textContent = "";
    structureArea.classList.add("hidden");
  }

  async function loadTables() {
    tableList.innerHTML =
      '<li><span class="block px-2 py-1 text-gray-500">Loading...</span></li>';
    const data = await executeQuery("SELECT name FROM SQLR_TABLES;", true);

    tableList.innerHTML = "";

    if (data && data.result && data.result.RowSet && data.result.RowSet.rows) {
      const tables = data.result.RowSet.rows.map((row) => row[0]).sort();
      if (tables.length > 0) {
        tables.forEach((tableName) => {
          const li = document.createElement("li");
          const a = document.createElement("a");
          a.className =
            "block px-2 py-1 rounded text-gray-700 hover:bg-gray-200 hover:text-gray-900";
          a.href = "#";
          a.textContent = tableName;
          a.addEventListener("click", (e) => {
            e.preventDefault();
            handleTableSelect(a, tableName);
          });
          li.appendChild(a);
          tableList.appendChild(li);
        });
      } else {
        tableList.innerHTML =
          '<li><span class="block px-2 py-1 text-gray-500">No tables found.</span></li>';
      }
    } else {
      tableList.innerHTML =
        '<li><span class="block px-2 py-1 text-red-600">Error loading tables.</span></li>';
      console.error("Error fetching tables:", data);
    }
  }

  async function handleTableSelect(linkElement, tableName) {
    tableList
      .querySelectorAll("a")
      .forEach((el) => el.classList.remove("bg-gray-200", "font-semibold"));
    linkElement.classList.add("bg-gray-200", "font-semibold");

    displayStatus(`Fetching structure for ${tableName}...`, "info");
    const structureQuery = `SELECT * FROM ${tableName};`;
    const data = await executeQuery(structureQuery, true);

    if (data && data.result && data.result.RowSet) {
      const columns = data.result.RowSet.columns.map((name) => ({ name }));
      displayStructure(tableName, columns);
      displayStatus(`Showing structure for ${tableName}.`, "secondary");
    } else {
      displayStructure(tableName, []);
      displayStatus(`Could not fetch structure for ${tableName}.`, "warning");
      console.error("Error fetching structure for", tableName, data);
    }
  }

  function updateHistoryDropdown() {
    historyList.innerHTML = "";
    if (queryHistory.length === 0) {
      historyList.innerHTML =
        '<li><span class="block px-3 py-2 text-sm text-gray-500">No history yet</span></li>';
      historyButton.disabled = true;
    } else {
      historyButton.disabled = false;
      queryHistory
        .slice()
        .reverse()
        .forEach((query) => {
          const li = document.createElement("li");
          const a = document.createElement("a");
          a.className =
            "block w-full px-3 py-2 text-sm text-gray-700 hover:bg-gray-100 text-left truncate";
          a.href = "#";
          a.textContent = query;
          a.title = query;
          a.addEventListener("click", (e) => {
            e.preventDefault();
            editor.setValue(query);
            editor.focus();
            historyList.classList.add("hidden");
          });
          li.appendChild(a);
          historyList.appendChild(li);
        });

      const hr = document.createElement("hr");
      hr.className = "my-1 border-gray-200";
      historyList.appendChild(hr);

      const liClear = document.createElement("li");
      const aClear = document.createElement("a");
      aClear.className =
        "block w-full px-3 py-2 text-sm text-red-600 hover:bg-red-50 text-left";
      aClear.href = "#";
      aClear.textContent = "Clear History";
      aClear.addEventListener("click", (e) => {
        e.preventDefault();
        if (confirm("Are you sure you want to clear the query history?")) {
          queryHistory = [];
          localStorage.removeItem("sqlrQueryHistory");
          updateHistoryDropdown();
          historyList.classList.add("hidden");
        }
      });
      liClear.appendChild(aClear);
      historyList.appendChild(liClear);
    }
  }

  function addToHistory(query) {
    if (
      queryHistory.length > 0 &&
      queryHistory[queryHistory.length - 1] === query
    ) {
      return;
    }
    const MAX_HISTORY = 20;
    queryHistory.push(query);
    if (queryHistory.length > MAX_HISTORY) {
      queryHistory.shift();
    }
    localStorage.setItem("sqlrQueryHistory", JSON.stringify(queryHistory));
    updateHistoryDropdown();
  }

  submitButton.addEventListener("click", () => {
    const query = editor.getValue().trim();
    executeQuery(query);
  });

  historyButton.addEventListener("click", (e) => {
    e.stopPropagation();
    historyList.classList.toggle("hidden");
  });

  document.addEventListener("click", (e) => {
    if (!historyButton.contains(e.target) && !historyList.contains(e.target)) {
      historyList.classList.add("hidden");
    }
  });

  editor.addKeyMap({
    "Ctrl-Enter": function (cm) {
      executeQuery(cm.getValue().trim());
    },
    "Cmd-Enter": function (cm) {
      executeQuery(cm.getValue().trim());
    },
  });

  loadTables();
  updateHistoryDropdown();
});
