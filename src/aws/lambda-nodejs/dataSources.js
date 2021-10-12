const NodeCache = require("node-cache");
const axios = require("axios");

const eodhistoricaldataApiToken = process.env.eodhistoricaldata_api_token;
const gnewsApiToken = process.env.gnews_api_token;

const cache = new NodeCache({ stdTTL: 100, checkperiod: 120 });

const cached = async (url, cacheTTL) => {
  const cacheKey = `GET_${url}`;

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

exports.eodFetch = async (urlParts, queryParams, cacheTTL) => {
  const params = new URLSearchParams({
    api_token: eodhistoricaldataApiToken,
    fmt: "json",
    ...queryParams,
  });
  const urlPart = urlParts.map((x) => encodeURIComponent(x)).join("/");
  const url = `https://eodhistoricaldata.com/api/${urlPart}?${params.toString()}`;
  return cached(url, cacheTTL);
};

exports.gnewsFetch = async (urlParts, queryParams, cacheTTL) => {
  const params = new URLSearchParams({
    token: gnewsApiToken,
    ...queryParams,
  });
  const urlPart = urlParts.map((x) => encodeURIComponent(x)).join("/");
  const url = `https://gnews.io/api/v4/${urlPart}?${params.toString()}`;

  const fetch = async (attemptsLeft) => {
    try {
      return await cached(url, cacheTTL);
    } catch (error) {
      if (error.response.status === 429 && attemptsLeft > 0) {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve(fetch(attemptsLeft - 1));
          }, 1000);
        });
      }

      throw error;
    }
  };

  return fetch(3);
};
