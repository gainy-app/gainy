const axios = require('axios');
const NodeCache = require('node-cache');
const moment = require('moment');
const { successfulResponse, badRequestResponse } = require('../response');

const token = process.env.eodhistoricaldata_api_token;

const cache = new NodeCache({ stdTTL: 100, checkperiod: 120 });

const eodFetch = async (urlParts, queryParams, cacheTTL) => {
  const params = new URLSearchParams({
    api_token: token,
    fmt: 'json',
    ...queryParams,
  });
  const urlPart = urlParts.map((x) => encodeURIComponent(x)).join('/');
  const url = `https://eodhistoricaldata.com/api/${urlPart}?${params.toString()}`;
  const cacheKey = `EOD_${url}`;

  if (cacheTTL && cache.has(cacheKey)) {
    const data = cache.get(cacheKey);
    if (data) {
      return data;
    }
  }

  const res = await axios.get(url);
  const { data } = res;

  cache.set(cacheKey, data, cacheTTL);

  return data;
};

const intradayData = async (symbol) => successfulResponse(
  (await eodFetch(['intraday', symbol], {
    interval: '5m',
    from: moment().subtract(1, 'week').startOf('minute').unix(),
  }, 60)).map((row) => {
    const {
      datetime, open, high, low, close, volume,
    } = row;
    return {
      datetime: moment(datetime).format(), open, high, low, close, volume,
    };
  }),
);

exports.fetchChartData = async (event) => {
  // TODO some elegant input validation
  const body = JSON.parse(event.body);
  if (!body) {
    return badRequestResponse('empty body');
  }
  const { input } = body;
  if (!input) {
    return badRequestResponse('input is unset');
  }

  const { symbol, period } = input;

  if (!symbol) {
    return badRequestResponse('symbol is unset');
  }
  if (!period) {
    return badRequestResponse('period is unset');
  }
  if (['1D', '1W', '1M', '3M', '1Y', '5Y', 'ALL'].indexOf(period) === -1) {
    return badRequestResponse('period is incorrect');
  }

  if (period === '1D') {
    return intradayData(symbol);
  }

  let from = null;
  let eodPeriod = null;
  let cacheTTL = null;
  switch (period) {
    case '1W':
      from = moment().subtract(1, 'week').startOf('day');
      eodPeriod = 'd';
      cacheTTL = 86400;
      break;
    case '1M':
      from = moment().subtract(1, 'month').startOf('day');
      eodPeriod = 'd';
      cacheTTL = 86400;
      break;
    case '3M':
      from = moment().subtract(3, 'months').startOf('week');
      eodPeriod = 'w';
      cacheTTL = 7 * 86400;
      break;
    case '1Y':
      from = moment().subtract(1, 'year').startOf('month');
      eodPeriod = 'm';
      cacheTTL = 30 * 86400;
      break;
    case '5Y':
      from = moment().subtract(5, 'year').startOf('month');
      eodPeriod = 'm';
      cacheTTL = 30 * 86400;
      break;
    case 'ALL':
      eodPeriod = 'm';
      cacheTTL = 30 * 86400;
      break;
    default:
      return badRequestResponse('period is incorrect');
  }

  const result = (await eodFetch(['eod', symbol], {
    period: eodPeriod,
    order: 'a',
    ...(from ? { from: from.format('YYYY-MM-DD') } : {}),
  }, cacheTTL)).map((row) => {
    const {
      date, open, high, low, close, volume,
    } = row;
    const datetime = moment(date).format();
    return {
      datetime, open, high, low, close, volume,
    };
  });

  return successfulResponse(result);
};
