const moment = require("moment");
const NodeCache = require("node-cache");
const { successfulResponse, badRequestResponse } = require("../response");
const { eodFetch } = require("../dataSources");
const { getInput } = require("../request");

const cacheTTL = 5 * 60;
const cache = new NodeCache({ stdTTL: cacheTTL, checkperiod: 120 });

exports.fetchLivePrices = async (event) => {
  let input;
  try {
    input = getInput(event);
  } catch (e) {
    return badRequestResponse(e.message);
  }

  const { symbols } = input;

  if (!symbols) {
    return badRequestResponse("symbol is unset");
  }
  if (!Array.isArray(symbols)) {
    return badRequestResponse("symbol is not array");
  }

  const cachedSymbols = symbols.filter((symbol) => cache.has(symbol));
  const cachedResult = Object.fromEntries(
    cachedSymbols.map((symbol) => [symbol, cache.get(symbol)])
  );

  const fetchedSymbols = symbols.filter(
    (symbol) => !(symbol in cachedResult) || !cachedResult[symbol]
  );
  fetchedSymbols.sort();

  const chunks = [];
  const chunkSize = 25; // after some tests i discovered this is the best chunk size, trust me!
  for (let i = 0; i < fetchedSymbols.length; i += chunkSize) {
    chunks.push(fetchedSymbols.slice(i, i + chunkSize));
  }

  const fetchedDataChunks = await Promise.all(
    chunks.map(async (chunk) => {
      switch (chunk.length) {
        case 0:
          return [];
        case 1:
          return [await eodFetch(["real-time", chunk[0]], {}, cacheTTL)];
        default:
          return eodFetch(
            ["real-time", chunk[0]],
            {
              s: chunk.slice(1).join(","),
            },
            cacheTTL
          );
      }
    })
  );

  const fetchedData = fetchedDataChunks.reduce((accumulator, current) =>
    accumulator.concat(current)
  );

  const isNA = (x) => x === "NA";

  const fetchedEntries = fetchedData.map((row, index) => {
    const { change, close, timestamp } = row; // Other fields: code, gmtoffset, high, low, open, previousClose
    const datetime = isNA(timestamp) ? null : moment(timestamp * 1000).format();
    return [
      fetchedSymbols[index],
      {
        symbol: fetchedSymbols[index],
        datetime,
        daily_change: isNA(change) ? null : change,
        daily_change_p: isNA(row.change_p) ? null : row.change_p,
        close: isNA(close) ? null : close,
      },
    ];
  });

  fetchedEntries.forEach((entry) => {
    cache.set(entry[0], entry[1]);
  });

  const result = {
    ...cachedResult,
    ...Object.fromEntries(fetchedEntries),
  };

  return successfulResponse(symbols.map((symbol) => result[symbol]));
};
