const { fetchChartData } = require("./functions/fetchChartData");
const { fetchNewsData } = require("./functions/fetchNewsData");
const { fetchLivePrices } = require("./functions/fetchLivePrices");

exports.fetchChartData = fetchChartData; // deprecated
exports.fetchNewsData = fetchNewsData;
exports.fetchLivePrices = fetchLivePrices; // deprecated
