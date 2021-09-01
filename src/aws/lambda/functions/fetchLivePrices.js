const moment = require('moment');
const NodeCache = require("node-cache");
const { successfulResponse, badRequestResponse } = require('../response');
const {eodFetch} = require("../dataSources");
const {getInput} = require("../request");

const cacheTTL = 5 * 60;
const cache = new NodeCache({ stdTTL: cacheTTL, checkperiod: 120 });

exports.fetchLivePrices = async (event) => {
  let input;
  try {
    input = getInput(event);
  }catch (e) {
    return badRequestResponse(e.message);
  }

  const { symbols } = input;

  if (!symbols) {
    return badRequestResponse('symbol is unset');
  }
  if (!Array.isArray(symbols)) {
    return badRequestResponse('symbol is not array');
  }

  const cachedSymbols = symbols.filter(symbol => cache.has(symbol));
  const cachedResult = Object.fromEntries(cachedSymbols.map(symbol => [symbol, cache.get(symbol)]));

  const fetchedSymbols = symbols.filter(symbol => !(symbol in cachedResult) || !cachedResult[symbol]);
  fetchedSymbols.sort();

  let fetchedData;
  switch(fetchedSymbols.length){
    case 0:
      fetchedData = [];
      break;
    case 1:
      fetchedData = [(await eodFetch(['real-time', fetchedSymbols[0]], {}, cacheTTL))];
      break;
    default:
      fetchedData = (await eodFetch(['real-time', fetchedSymbols[0]], {
        s: fetchedSymbols.slice(1).join(','),
      }, cacheTTL));
      break;
  }

  const fetchedEntries = fetchedData.map((row, index) => {
    const {change, close, timestamp} = row; // Other fields: code, gmtoffset, high, low, open, previousClose
    const datetime = moment(timestamp * 1000).format();
    return [
      fetchedSymbols[index],
      {
        symbol: fetchedSymbols[index],
        datetime,
        daily_change : change,
        daily_change_p : row.change_p,
        close
      }
    ];
  });

  fetchedEntries.forEach(entry => {
    cache.set(entry[0], entry[1]);
  });

  const result = {
    ...cachedResult,
    ...Object.fromEntries(fetchedEntries)
  };

  return successfulResponse(Object.entries(result).map(entry => entry[1]));
};
