const moment = require("moment");
const { successfulResponse, badRequestResponse } = require("../response");
const { getInput } = require("../request");
const { gnewsFetch } = require("../dataSources");

const maxLimit = 10; // 10 for free tier
const defaultLimit = 5;
const cacheTTL = 15 * 60;

exports.fetchNewsData = async (event) => {
  let input;
  try {
    input = getInput(event);
  } catch (e) {
    return badRequestResponse(e.message);
  }

  const { symbol, limit } = input;
  if (!symbol) {
    return badRequestResponse("symbol is unset");
  }
  if (limit && limit > maxLimit) {
    return badRequestResponse("max limit is 10");
  }

  const result = (
    await gnewsFetch(
      ["search"],
      {
        q: symbol,
        lang: 'en',
        max: limit || defaultLimit,
      },
      cacheTTL
    )
  ).articles.map((row) => {
    const { publishedAt, title, description, url, image, source } = row;
    const datetime = moment(publishedAt).format();
    return {
      datetime,
      title,
      description,
      url,
      imageUrl: image,
      sourceName: source.name,
      sourceUrl: source.url,
    };
  });

  return successfulResponse(result);
};
