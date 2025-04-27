document.addEventListener("DOMContentLoaded", () => {
  const queryInput = document.getElementById("query-input");
  const submitButton = document.getElementById("submit-button");
  const resultOutput = document.getElementById("result-output");
  const resultTableContainer = document.getElementById(
    "result-table-container"
  );

  submitButton.addEventListener("click", async () => {
    const query = queryInput.value.trim();
    if (!query) {
      resultOutput.textContent = "Please enter a query.";
      resultTableContainer.innerHTML = "";
      return;
    }

    resultOutput.textContent = "Executing...";
    resultTableContainer.innerHTML = "";
    submitButton.disabled = true;

    try {
      const response = await fetch("/api/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query: query }),
      });

      const data = await response.json();

      resultOutput.textContent = `Status: ${data.message || "No message"}\n`;

      if (data.error) {
        resultOutput.textContent += `Details: ${JSON.stringify(
          data.details || "Unknown error",
          null,
          2
        )}`;
        resultTableContainer.innerHTML = "";
      } else if (data.result) {
        if (data.result.RowSet) {
          const rowSet = data.result.RowSet;
          if (rowSet.rows && rowSet.rows.length > 0) {
            displayTable(rowSet);
            resultOutput.textContent += `(${rowSet.rows.length} row(s) returned)`;
          } else {
            displayTable(rowSet);
            resultOutput.textContent += `(0 rows returned)`;
          }
        } else if (data.result.Msg) {
          resultOutput.textContent += `Message: ${data.result.Msg}`;
          resultTableContainer.innerHTML = "";
        } else {
          resultOutput.textContent += `Result: ${JSON.stringify(
            data.result,
            null,
            2
          )}`;
          resultTableContainer.innerHTML = "";
        }
      } else {
        resultOutput.textContent +=
          "Execution finished, but no result data received.";
        resultTableContainer.innerHTML = "";
      }
    } catch (error) {
      console.error("Error executing query:", error);
      resultOutput.textContent = `Network or fetch error: ${error.message}`;
    } finally {
      submitButton.disabled = false;
    }
  });

  function displayTable(result) {
    const table = document.createElement("table");
    const thead = table.createTHead();
    const tbody = table.createTBody();

    const headerRow = thead.insertRow();
    result.columns.forEach((colName) => {
      const th = document.createElement("th");
      th.textContent = colName;
      headerRow.appendChild(th);
    });

    result.rows.forEach((rowData) => {
      const row = tbody.insertRow();
      rowData.forEach((cellData) => {
        const cell = row.insertCell();

        cell.textContent = cellData;
      });
    });

    resultTableContainer.innerHTML = "";
    resultTableContainer.appendChild(table);
  }
});
