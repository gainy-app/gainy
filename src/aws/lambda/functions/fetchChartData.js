const moment = require('moment');
const { successfulResponse, badRequestResponse } = require('../response');
const {eodFetch} = require("../dataSources");
const {getInput} = require("../request");

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
  let input;
  try {
    input = getInput(event);
  }catch (e) {
    return badRequestResponse(e.message);
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
