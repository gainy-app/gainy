create table raw_data.coingecko_coin
(
    additional_notices              jsonb,
    asset_platform_id               varchar,
    block_time_in_minutes           numeric,
    categories                      jsonb,
    coingecko_rank                  numeric,
    coingecko_score                 double precision,
    community_data                  jsonb,
    community_score                 double precision,
    country_origin                  varchar,
    description                     jsonb,
    developer_data                  jsonb,
    developer_score                 double precision,
    genesis_date                    varchar,
    hashing_algorithm               varchar,
    id                              varchar not null
        primary key,
    image                           jsonb,
    last_updated                    varchar,
    links                           jsonb,
    liquidity_score                 double precision,
    market_cap_rank                 numeric,
    market_data                     jsonb,
    name                            varchar,
    platforms                       jsonb,
    public_interest_score           double precision,
    public_interest_stats           jsonb,
    public_notice                   varchar,
    sentiment_votes_down_percentage double precision,
    sentiment_votes_up_percentage   double precision,
    status_updates                  jsonb,
    symbol                          varchar,
    contract_address                varchar,
    ico_data                        jsonb
);

INSERT INTO raw_data.coingecko_coin (additional_notices, asset_platform_id, block_time_in_minutes, categories, coingecko_rank, coingecko_score, community_data, community_score, country_origin, description, developer_data, developer_score, genesis_date, hashing_algorithm, id, image, last_updated, links, liquidity_score, market_cap_rank, market_data, name, platforms, public_interest_score, public_interest_stats, public_notice, sentiment_votes_down_percentage, sentiment_votes_up_percentage, status_updates, symbol, contract_address, ico_data)
VALUES ('[]', null, 10, '["Cryptocurrency"]', 1, 80.575, '{"facebook_likes": null, "twitter_followers": 4901889, "reddit_subscribers": 4071697, "reddit_average_posts_48h": 6.222, "reddit_accounts_active_48h": 1706, "reddit_average_comments_48h": 602.222, "telegram_channel_user_count": null}', 72.094, null, '{"en": "Bitcoin is the first successful internet money based on peer-to-peer technology; whereby no central bank or authority is involved in the transaction and production of the Bitcoin currency. It was created by an anonymous individual/group under the name, Satoshi Nakamoto. The source code is available publicly as an open source project, anybody can look at it and be part of the developmental process.\r\n\r\nBitcoin is changing the way we see money as we speak. The idea was to produce a means of exchange, independent of any central authority, that could be transferred electronically in a secure, verifiable and immutable way. It is a decentralized peer-to-peer internet currency making mobile payment easy, very low transaction fees, protects your identity, and it works anywhere all the time with no central authority and banks.\r\n\r\nBitcoin is designed to have only 21 million BTC ever created, thus making it a deflationary currency. Bitcoin uses the <a href=\"https://www.coingecko.com/en?hashing_algorithm=SHA-256\">SHA-256</a> hashing algorithm with an average transaction confirmation time of 10 minutes. Miners today are mining Bitcoin using ASIC chip dedicated to only mining Bitcoin, and the hash rate has shot up to peta hashes.\r\n\r\nBeing the first successful online cryptography currency, Bitcoin has inspired other alternative currencies such as <a href=\"https://www.coingecko.com/en/coins/litecoin\">Litecoin</a>, <a href=\"https://www.coingecko.com/en/coins/peercoin\">Peercoin</a>, <a href=\"https://www.coingecko.com/en/coins/primecoin\">Primecoin</a>, and so on.\r\n\r\nThe cryptocurrency then took off with the innovation of the turing-complete smart contract by <a href=\"https://www.coingecko.com/en/coins/ethereum\">Ethereum</a> which led to the development of other amazing projects such as <a href=\"https://www.coingecko.com/en/coins/eos\">EOS</a>, <a href=\"https://www.coingecko.com/en/coins/tron\">Tron</a>, and even crypto-collectibles such as <a href=\"https://www.coingecko.com/buzz/ethereum-still-king-dapps-cryptokitties-need-1-billion-on-eos\">CryptoKitties</a>."}', '{"forks": 32033, "stars": 63008, "subscribers": 3909, "total_issues": 6817, "closed_issues": 6215, "commit_count_4_weeks": 347, "pull_requests_merged": 9610, "pull_request_contributors": 775, "code_additions_deletions_4_weeks": {"additions": 2365, "deletions": -1785}, "last_4_weeks_commit_activity_series": [2, 7, 7, 12, 11, 3, 0, 0, 7, 9, 5, 12, 4, 3, 0, 7, 10, 4, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0]}', 98.94, '2009-01-03', 'SHA-256', 'bitcoin', '{"large": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1547033579", "small": "https://assets.coingecko.com/coins/images/1/small/bitcoin.png?1547033579", "thumb": "https://assets.coingecko.com/coins/images/1/thumb/bitcoin.png?1547033579"}', '2022-04-05T10:50:22.169Z', '{"chat_url": ["", "", ""], "homepage": ["http://www.bitcoin.org", "", ""], "repos_url": {"github": ["https://github.com/bitcoin/bitcoin", "https://github.com/bitcoin/bips"], "bitbucket": []}, "subreddit_url": "https://www.reddit.com/r/Bitcoin/", "blockchain_site": ["https://blockchair.com/bitcoin/", "https://btc.com/", "https://btc.tokenview.com/", "", "", "", "", "", "", ""], "announcement_url": ["", ""], "facebook_username": "bitcoins", "official_forum_url": ["https://bitcointalk.org/", "", ""], "twitter_screen_name": "bitcoin", "telegram_channel_identifier": "", "bitcointalk_thread_identifier": null}', 99.898, 1, '{"ath": {"aed": 253608, "ars": 6913791, "aud": 93482, "bch": 139.985, "bdt": 5922005, "bhd": 26031, "bmd": 69045, "bnb": 143062, "brl": 380542, "btc": 1.003301, "cad": 85656, "chf": 62992, "clp": 55165171, "cny": 440948, "czk": 1505245, "dkk": 444134, "dot": 5526, "eos": 20560, "eth": 624.203, "eur": 59717, "gbp": 51032, "hkd": 537865, "huf": 21673371, "idr": 984115318, "ils": 216131, "inr": 5128383, "jpy": 7828814, "krw": 81339064, "kwd": 20832, "lkr": 14190616, "ltc": 400.978, "mmk": 126473151, "mxn": 1409247, "myr": 286777, "ngn": 28379648, "nok": 591777, "nzd": 97030, "php": 3454759, "pkr": 11814869, "pln": 275506, "rub": 6075508, "sar": 258938, "sek": 596346, "sgd": 93063, "thb": 2258593, "try": 850326, "twd": 1914232, "uah": 1815814, "usd": 69045, "vef": 8618768857, "vnd": 1563347910, "xag": 2815.08, "xau": 37.72, "xdr": 48913, "xlm": 275874, "xrp": 159288, "yfi": 2.474634, "zar": 1057029, "bits": 1058236, "link": 3031, "sats": 105823579}, "atl": {"aed": 632.31, "ars": 1478.98, "aud": 72.61, "bch": 3.513889, "bdt": 9390.25, "bhd": 45.91, "bmd": 121.77, "bnb": 81.254, "brl": 149.66, "btc": 0.99895134, "cad": 69.81, "chf": 63.26, "clp": 107408, "cny": 407.23, "czk": 4101.56, "dkk": 382.47, "dot": 991.882, "eos": 908.141, "eth": 6.779735, "eur": 51.3, "gbp": 43.9, "hkd": 514.37, "huf": 46598, "idr": 658780, "ils": 672.18, "inr": 3993.42, "jpy": 6641.83, "krw": 75594, "kwd": 50.61, "lkr": 22646, "ltc": 20.707835, "mmk": 117588, "mxn": 859.32, "myr": 211.18, "ngn": 4289706, "nok": 1316.03, "nzd": 84.85, "php": 2880.5, "pkr": 17315.84, "pln": 220.11, "rub": 2206.43, "sar": 646.04, "sek": 443.81, "sgd": 84.47, "thb": 5644.35, "try": 392.91, "twd": 1998.66, "uah": 553.37, "usd": 67.81, "vef": 766.19, "vnd": 3672339, "xag": 3.37, "xau": 0.0531, "xdr": 44.39, "xlm": 21608, "xrp": 9908, "yfi": 0.23958075, "zar": 666.26, "bits": 950993, "link": 598.477, "sats": 95099268}, "roi": null, "low_24h": {"aed": 166304, "ars": 5051726, "aud": 60040, "bch": 121.569, "bdt": 3903276, "bhd": 17074.18, "bmd": 45277, "bnb": 102.395, "brl": 209018, "btc": 1.0, "cad": 56580, "chf": 41944, "clp": 35271065, "cny": 288118, "czk": 1004709, "dkk": 307039, "dot": 2022, "eos": 15705, "eth": 13.141735, "eur": 41283, "gbp": 34558, "hkd": 354728, "huf": 15234675, "idr": 648652544, "ils": 145328, "inr": 3415360, "jpy": 5558815, "krw": 55012674, "kwd": 13775.37, "lkr": 13349260, "ltc": 367.589, "mmk": 80493377, "mxn": 896280, "myr": 190957, "ngn": 18822705, "nok": 394775, "nzd": 65092, "php": 2325785, "pkr": 8309528, "pln": 190790, "rub": 3778677, "sar": 169856, "sek": 426864, "sgd": 61441, "thb": 1515272, "try": 665301, "twd": 1296721, "uah": 1330775, "usd": 45277, "vef": 4533.62, "vnd": 1034089672, "xag": 1846.25, "xau": 23.44, "xdr": 32516, "xlm": 196136, "xrp": 55126, "yfi": 1.917156, "zar": 661116, "bits": 998580, "link": 2637, "sats": 99858046}, "ath_date": {"aed": "2021-11-10T14:24:11.849Z", "ars": "2021-11-10T14:24:11.849Z", "aud": "2021-11-10T14:24:11.849Z", "bch": "2022-03-09T08:49:19.868Z", "bdt": "2021-11-10T14:24:11.849Z", "bhd": "2021-11-10T14:24:11.849Z", "bmd": "2021-11-10T14:24:11.849Z", "bnb": "2017-10-19T00:00:00.000Z", "brl": "2021-11-09T04:09:45.771Z", "btc": "2019-10-15T16:00:56.136Z", "cad": "2021-11-10T14:24:11.849Z", "chf": "2021-11-10T17:30:22.767Z", "clp": "2021-11-09T04:09:45.771Z", "cny": "2021-11-10T14:24:11.849Z", "czk": "2021-11-10T14:24:11.849Z", "dkk": "2021-11-10T14:24:11.849Z", "dot": "2020-12-27T11:42:47.567Z", "eos": "2022-03-09T16:14:54.259Z", "eth": "2015-10-20T00:00:00.000Z", "eur": "2021-11-10T14:24:11.849Z", "gbp": "2021-11-10T14:24:11.849Z", "hkd": "2021-11-10T14:24:11.849Z", "huf": "2021-11-10T16:54:53.781Z", "idr": "2021-11-10T14:24:11.849Z", "ils": "2021-10-20T14:54:17.702Z", "inr": "2021-11-10T14:24:11.849Z", "jpy": "2021-11-10T14:24:11.849Z", "krw": "2021-11-10T14:24:11.849Z", "kwd": "2021-11-10T14:24:11.849Z", "lkr": "2022-03-29T12:14:23.745Z", "ltc": "2022-03-02T15:32:41.298Z", "mmk": "2021-10-20T14:54:17.702Z", "mxn": "2021-11-10T17:30:22.767Z", "myr": "2021-11-10T14:24:11.849Z", "ngn": "2021-11-09T04:09:45.771Z", "nok": "2021-11-10T17:30:22.767Z", "nzd": "2021-11-10T14:24:11.849Z", "php": "2021-11-10T14:24:11.849Z", "pkr": "2021-11-10T14:24:11.849Z", "pln": "2021-11-10T14:24:11.849Z", "rub": "2022-03-07T16:43:46.826Z", "sar": "2021-11-10T14:24:11.849Z", "sek": "2021-11-10T17:30:22.767Z", "sgd": "2021-11-10T14:24:11.849Z", "thb": "2021-11-10T14:24:11.849Z", "try": "2021-12-20T16:44:25.022Z", "twd": "2021-11-10T14:24:11.849Z", "uah": "2021-04-14T06:52:46.198Z", "usd": "2021-11-10T14:24:11.849Z", "vef": "2021-01-03T12:04:17.372Z", "vnd": "2021-11-10T14:24:11.849Z", "xag": "2021-11-09T04:09:45.771Z", "xau": "2021-10-20T14:54:17.702Z", "xdr": "2021-11-10T14:24:11.849Z", "xlm": "2021-01-03T07:50:39.913Z", "xrp": "2021-01-03T07:54:40.240Z", "yfi": "2021-12-15T16:09:27.012Z", "zar": "2021-11-10T17:49:04.400Z", "bits": "2021-05-19T16:00:11.072Z", "link": "2022-03-09T22:39:36.450Z", "sats": "2021-05-19T16:00:11.072Z"}, "atl_date": {"aed": "2015-01-14T00:00:00.000Z", "ars": "2015-01-14T00:00:00.000Z", "aud": "2013-07-05T00:00:00.000Z", "bch": "2017-08-02T00:00:00.000Z", "bdt": "2013-09-08T00:00:00.000Z", "bhd": "2013-09-08T00:00:00.000Z", "bmd": "2013-09-08T00:00:00.000Z", "bnb": "2021-05-13T07:09:55.887Z", "brl": "2013-07-05T00:00:00.000Z", "btc": "2019-10-21T00:00:00.000Z", "cad": "2013-07-05T00:00:00.000Z", "chf": "2013-07-05T00:00:00.000Z", "clp": "2015-01-14T00:00:00.000Z", "cny": "2013-07-05T00:00:00.000Z", "czk": "2015-01-14T00:00:00.000Z", "dkk": "2013-07-05T00:00:00.000Z", "dot": "2021-05-19T11:04:48.978Z", "eos": "2019-04-11T00:00:00.000Z", "eth": "2017-06-12T00:00:00.000Z", "eur": "2013-07-05T00:00:00.000Z", "gbp": "2013-07-05T00:00:00.000Z", "hkd": "2013-07-05T00:00:00.000Z", "huf": "2015-01-14T00:00:00.000Z", "idr": "2013-07-05T00:00:00.000Z", "ils": "2015-01-14T00:00:00.000Z", "inr": "2013-07-05T00:00:00.000Z", "jpy": "2013-07-05T00:00:00.000Z", "krw": "2013-07-05T00:00:00.000Z", "kwd": "2015-01-14T00:00:00.000Z", "lkr": "2015-01-14T00:00:00.000Z", "ltc": "2013-11-28T00:00:00.000Z", "mmk": "2013-09-08T00:00:00.000Z", "mxn": "2013-07-05T00:00:00.000Z", "myr": "2013-07-05T00:00:00.000Z", "ngn": "2020-10-15T09:39:31.080Z", "nok": "2015-01-14T00:00:00.000Z", "nzd": "2013-07-05T00:00:00.000Z", "php": "2013-07-05T00:00:00.000Z", "pkr": "2015-01-14T00:00:00.000Z", "pln": "2013-07-05T00:00:00.000Z", "rub": "2013-07-05T00:00:00.000Z", "sar": "2015-01-14T00:00:00.000Z", "sek": "2013-07-05T00:00:00.000Z", "sgd": "2013-07-05T00:00:00.000Z", "thb": "2015-01-14T00:00:00.000Z", "try": "2015-01-14T00:00:00.000Z", "twd": "2013-07-05T00:00:00.000Z", "uah": "2013-07-06T00:00:00.000Z", "usd": "2013-07-06T00:00:00.000Z", "vef": "2013-09-08T00:00:00.000Z", "vnd": "2015-01-14T00:00:00.000Z", "xag": "2013-07-05T00:00:00.000Z", "xau": "2013-07-05T00:00:00.000Z", "xdr": "2013-07-05T00:00:00.000Z", "xlm": "2018-11-20T00:00:00.000Z", "xrp": "2018-12-25T00:00:00.000Z", "yfi": "2020-09-12T20:09:36.122Z", "zar": "2013-07-05T00:00:00.000Z", "bits": "2021-05-19T13:14:13.071Z", "link": "2020-08-16T08:13:13.338Z", "sats": "2021-05-19T13:14:13.071Z"}, "high_24h": {"aed": 172093, "ars": 5228310, "aud": 62050, "bch": 124.887, "bdt": 4049084, "bhd": 17665.18, "bmd": 46853, "bnb": 104.373, "brl": 215711, "btc": 1.0, "cad": 58490, "chf": 43419, "clp": 36500091, "cny": 298165, "czk": 1040283, "dkk": 317816, "dot": 2085, "eos": 16601, "eth": 13.330767, "eur": 42732, "gbp": 35710, "hkd": 367067, "huf": 15783185, "idr": 672232569, "ils": 150447, "inr": 3536416, "jpy": 5751220, "krw": 56897068, "kwd": 14258.95, "lkr": 13848251, "ltc": 374.486, "mmk": 83501328, "mxn": 928112, "myr": 197510, "ngn": 19523642, "nok": 408737, "nzd": 67391, "php": 2404132, "pkr": 8661806, "pln": 197672, "rub": 3969016, "sar": 175768, "sek": 442037, "sgd": 63562, "thb": 1567605, "try": 688491, "twd": 1341479, "uah": 1380618, "usd": 46853, "vef": 4691.42, "vnd": 1071088822, "xag": 1916.09, "xau": 24.31, "xdr": 33648, "xlm": 202358, "xrp": 56708, "yfi": 1.985164, "zar": 683151, "bits": 1001260, "link": 2703, "sats": 100125981}, "market_cap": {"aed": 3258383485367, "ars": 98982689250420, "aud": 1163737218918, "bch": 2348383227, "bdt": 76665104585102, "bhd": 334478420074, "bmd": 887116539555, "bnb": 1950770746, "brl": 4076566634215, "btc": 19003425, "cad": 1105724232814, "chf": 821594999060, "clp": 691090397809200, "cny": 5645432234417, "czk": 19707702886930, "dkk": 6021070200577, "dot": 38738073065, "eos": 311023354224, "eth": 252034278, "eur": 809546182219, "gbp": 675965947926, "hkd": 6948650786850, "huf": 300151780905643, "idr": 12735400686018478, "ils": 2843954574282, "inr": 66798019580659, "jpy": 109052792649174, "krw": 1076612156038802, "kwd": 270142067276, "lkr": 262201911171692, "ltc": 7023279823, "mmk": 1581008835517146, "mxn": 17588354860438, "myr": 3735204189795, "ngn": 369659404809136, "nok": 7734005301036, "nzd": 1268188094519, "php": 45469605341606, "pkr": 164002077428167, "pln": 3747998184528, "rub": 74983529941433, "sar": 3328046972985, "sek": 8329249853678, "sgd": 1202952205549, "thb": 29671103908306, "try": 13060661965246, "twd": 25388832690899, "uah": 26140535326273, "usd": 887116539555, "vef": 88826979106, "vnd": 20288671005680272, "xag": 35996536697, "xau": 460094122, "xdr": 637787113414, "xlm": 3845772834754, "xrp": 1071376559537, "yfi": 37034043, "zar": 12900710417582, "bits": 19007985727355, "link": 51281172150, "sats": 1900798572735538}, "max_supply": 21000000.0, "last_updated": "2022-04-05T10:50:22.169Z", "total_supply": 21000000.0, "total_volume": {"aed": 92264989949, "ars": 2802812152044, "aud": 32952598517, "bch": 66487245, "bdt": 2170863293330, "bhd": 9471152860, "bmd": 25119756153, "bnb": 55123840, "brl": 115432815450, "btc": 538177, "cad": 31309892063, "chf": 23264436083, "clp": 19569043635904, "cny": 159857104207, "czk": 558046963147, "dkk": 170493738393, "dot": 1097578875, "eos": 8800607246, "eth": 7134603, "eur": 22923259555, "gbp": 19140776913, "hkd": 196759281983, "huf": 8499153390869, "idr": 360617963345265, "ils": 80529944185, "inr": 1891465087794, "jpy": 3087959064015, "krw": 30485549107984, "kwd": 7649392784, "lkr": 7424557854373, "ltc": 198900710, "mmk": 44768138855732, "mxn": 498035112105, "myr": 105766733282, "ngn": 10467344136258, "nok": 218997525788, "nzd": 35910248846, "php": 1287525761981, "pkr": 4643913183781, "pln": 106129010406, "rub": 2123247514435, "sar": 94237594160, "sek": 235852580730, "sgd": 34063017337, "thb": 840172470849, "try": 369828121939, "twd": 718914886342, "uah": 740200237317, "usd": 25119756153, "vef": 2515241184, "vnd": 574497763944162, "xag": 1019284597, "xau": 13028110, "xdr": 18059697968, "xlm": 108893508535, "xrp": 30324700321, "yfi": 1048412, "zar": 365298904306, "bits": 538176974972, "link": 1451017782, "sats": 53817697497158}, "current_price": {"aed": 171469, "ars": 5208847, "aud": 61240, "bch": 123.562, "bdt": 4034410, "bhd": 17601.53, "bmd": 46683, "bnb": 102.444, "brl": 214524, "btc": 1.0, "cad": 58187, "chf": 43235, "clp": 36367814, "cny": 297084, "czk": 1037095, "dkk": 316852, "dot": 2040, "eos": 16355, "eth": 13.259202, "eur": 42601, "gbp": 35572, "hkd": 365665, "huf": 15795132, "idr": 670185391, "ils": 149660, "inr": 3515167, "jpy": 5738774, "krw": 56655441, "kwd": 14215.91, "lkr": 13798065, "ltc": 369.644, "mmk": 83198719, "mxn": 925566, "myr": 196561, "ngn": 19452889, "nok": 406993, "nzd": 66737, "php": 2392784, "pkr": 8630415, "pln": 197234, "rub": 3945920, "sar": 175135, "sek": 438317, "sgd": 63304, "thb": 1561407, "try": 687302, "twd": 1336057, "uah": 1375615, "usd": 46683, "vef": 4674.41, "vnd": 1067667303, "xag": 1894.28, "xau": 24.21, "xdr": 33563, "xlm": 202372, "xrp": 56357, "yfi": 1.948406, "zar": 678885, "bits": 1000167, "link": 2697, "sats": 100016744}, "market_cap_rank": 1, "fdv_to_tvl_ratio": null, "price_change_24h": 719.358, "mcap_to_tvl_ratio": null, "circulating_supply": 19003425.0, "total_value_locked": null, "ath_change_percentage": {"aed": -32.43595, "ars": -24.70822, "aud": -34.52854, "bch": -11.7232, "bdt": -31.92213, "bhd": -32.43492, "bmd": -32.43421, "bnb": -99.9283, "brl": -43.67112, "btc": -0.32896, "cad": -32.09822, "chf": -31.42082, "clp": -34.12102, "cny": -32.6733, "czk": -31.18428, "dkk": -28.73702, "dot": -63.19655, "eos": -20.36438, "eth": -97.87521, "eur": -28.74305, "gbp": -30.34237, "hkd": -32.06507, "huf": -27.22684, "idr": -31.97364, "ils": -30.831, "inr": -31.50381, "jpy": -26.72176, "krw": -30.41086, "kwd": -31.81142, "lkr": -2.83461, "ltc": -7.83674, "mmk": -34.26253, "mxn": -34.39553, "myr": -31.50697, "ngn": -31.50297, "nok": -31.20273, "nzd": -31.26551, "php": -30.78613, "pkr": -27.00427, "pln": -28.51174, "rub": -35.48429, "sar": -32.41896, "sek": -26.54854, "sgd": -32.03634, "thb": -30.91222, "try": -19.24465, "twd": -30.28034, "uah": -24.29577, "usd": -32.43421, "vef": -99.99995, "vnd": -31.75435, "xag": -32.7011, "xau": -35.81532, "xdr": -31.43154, "xlm": -26.6848, "xrp": -64.54549, "yfi": -21.22483, "zar": -35.82915, "bits": -5.48758, "link": -10.86853, "sats": -5.48758}, "atl_change_percentage": {"aed": 26998.56967, "ars": 351867.33995, "aud": 84192.46051, "bch": 3416.73167, "bdt": 42833.60576, "bhd": 38206.39594, "bmd": 38210.46018, "bnb": 26.24626, "brl": 143129.45879, "btc": 0.10498, "cad": 83218.85212, "chf": 68187.62187, "clp": 33735.64061, "cny": 72802.00037, "czk": 25154.91462, "dkk": 82652.93436, "dot": 105.04166, "eos": 1702.94624, "eth": 95.62723, "eur": 82850.7119, "gbp": 80870.20193, "hkd": 70937.3296, "huf": 33747.7668, "idr": 101520.84562, "ils": 22140.29169, "inr": 87863.38907, "jpy": 86274.0124, "krw": 74778.04176, "kwd": 27966.63584, "lkr": 60785.74781, "ltc": 1684.61213, "mmk": 70604.72658, "mxn": 107488.63688, "myr": 92913.67141, "ngn": 353.15964, "nok": 30836.05271, "nzd": 78496.88718, "php": 82912.33581, "pkr": 49706.1161, "pln": 89379.36475, "rub": 177546.76366, "sar": 26987.04316, "sek": 98597.44127, "sgd": 74781.22125, "thb": 27545.56033, "try": 174669.01354, "twd": 66674.67837, "uah": 248314.83128, "usd": 68697.13219, "vef": 509.65718, "vnd": 28952.78655, "xag": 56122.20505, "xau": 45488.48856, "xdr": 75453.37975, "xlm": 836.01863, "xrp": 469.99303, "yfi": 713.67024, "zar": 101707.12083, "bits": 5.17055, "link": 351.37091, "sats": 5.17055}, "market_cap_change_24h": 15294979961, "fully_diluted_valuation": {"aed": 3600722143124, "ars": 109382202116662, "aud": 1286004054390, "bch": 2595113658, "bdt": 84719843727494, "bhd": 369620045942, "bmd": 980320512257, "bnb": 2155726437, "brl": 4504866849977, "btc": 21000000, "cad": 1221895994490, "chf": 907915019543, "clp": 763699088663922, "cny": 6238563675904, "czk": 21778272107555, "dkk": 6653667652653, "dot": 42808048253, "eos": 343700698095, "eth": 278513997, "eur": 894600306345, "gbp": 746985604250, "hkd": 7678703524436, "huf": 331686914280900, "idr": 14073432257942348, "ils": 3142751691336, "inr": 73816083742474, "jpy": 120510310411552, "krw": 1189725287773906, "kwd": 298524261431, "lkr": 289749881119090, "ltc": 7761173382, "mmk": 1747115877577862, "mxn": 19436256994157, "myr": 4127639516860, "ngn": 408497284094412, "nok": 8546570490412, "nzd": 1401428952144, "php": 50246821937294, "pkr": 181232784405522, "pln": 4141777699288, "rub": 82861596200164, "sar": 3677704752311, "sek": 9204353790290, "sgd": 1329339122634, "thb": 32788467451231, "try": 14432866773762, "twd": 28056283880873, "uah": 28886963368536, "usd": 980320512257, "vef": 98159492892, "vnd": 22420279034925848, "xag": 39778475229, "xau": 508433430, "xdr": 704795550364, "xlm": 4249824941022, "xrp": 1183939618794, "yfi": 40924986, "zar": 14256110083799, "bits": 21005039895412, "link": 56668974943, "sats": 2100503989541164}, "price_change_percentage_1y": -19.82755, "price_change_percentage_7d": -0.66267, "price_change_percentage_14d": 13.69068, "price_change_percentage_24h": 1.56504, "price_change_percentage_30d": 18.29634, "price_change_percentage_60d": 25.82684, "price_change_24h_in_currency": {"aed": 2644.04, "ars": 101362, "aud": 7.38, "bch": -0.29538991044, "bdt": 64556, "bhd": 268.01, "bmd": 719.36, "bnb": -1.212235622343, "brl": 386.94, "btc": 0.0, "cad": 716.45, "chf": 615.16, "clp": 377924, "cny": 4596.24, "czk": 20778, "dkk": 6194.73, "dot": -10.053190271185, "eos": -237.764658099675, "eth": -0.0221312393, "eur": 837.14, "gbp": 506.91, "hkd": 5524.31, "huf": 367558, "idr": 10860249, "ils": 2055.23, "inr": 44257, "jpy": 97773, "krw": 763926, "kwd": 228.66, "lkr": 103275, "ltc": 2.029948, "mmk": 1328426, "mxn": 14330.56, "myr": 2707.11, "ngn": 354805, "nok": 5504.14, "nzd": 373.75, "php": 31953, "pkr": 137909, "pln": 3313.31, "rub": 36397, "sar": 2706.28, "sek": 4896.73, "sgd": 912.27, "thb": 20259, "try": 11128.6, "twd": 19254.7, "uah": 21969, "usd": 719.36, "vef": 72.03, "vnd": 17539355, "xag": 29.83, "xau": 0.362976, "xdr": 553.4, "xlm": 4950, "xrp": 825.146, "yfi": 0.03125041, "zar": 5432.41, "bits": -522.115518232575, "link": 57.154, "sats": -52211.5518232584}, "price_change_percentage_200d": -2.49797, "market_cap_change_percentage_24h": 1.75437, "market_cap_change_24h_in_currency": {"aed": 56213410733, "ars": 2109441554896, "aud": 2712396112, "bch": -5613315.452764034, "bdt": 1367104879566, "bhd": 5699688984, "bmd": 15294979961, "bnb": -18247091.906177282, "brl": 15273081004, "btc": 1032, "cad": 16009182653, "chf": 13071171979, "clp": 8454116647279, "cny": 97682922098, "czk": 423533081651, "dkk": 126597454007, "dot": -236503056.3673935, "eos": -4083665026.2261353, "eth": -269546.93391525745, "eur": 17096129233, "gbp": 10786149852, "hkd": 117601934348, "huf": 7403919648460, "idr": 229898340023500, "ils": 44073481000, "inr": 931892906955, "jpy": 2036696209039, "krw": 16492902106577, "kwd": 4857228550, "lkr": 2446720127301, "ltc": 41914356, "mmk": 28138531255827, "mxn": 305277080895, "myr": 58296762208, "ngn": 7417546797924, "nok": 112521175171, "nzd": 9750289751, "php": 702438334382, "pkr": 2920922486253, "pln": 65927829665, "rub": 985065708984, "sar": 57513882577, "sek": 106546246638, "sgd": 19628802713, "thb": 438964503454, "try": 238167654101, "twd": 412451869055, "uah": 465335471478, "usd": 15294979961, "vef": 1531486343, "vnd": 370426555324744, "xag": 642141111, "xau": 7671042, "xdr": 11683229462, "xlm": 92440539137, "xrp": 15108623371, "yfi": 582220, "zar": 125647558386, "bits": -3265088356.1757812, "link": 1128683001, "sats": -326508835617.75}, "price_change_percentage_1h_in_currency": {"aed": 0.11566, "ars": 0.12516, "aud": 0.28399, "bch": -0.18123, "bdt": 0.11825, "bhd": 0.10497, "bmd": 0.11825, "bnb": -0.14578, "brl": 0.11389, "btc": 0.0, "cad": 0.18416, "chf": 0.24467, "clp": 0.11825, "cny": 0.11825, "czk": 0.29414, "dkk": 0.31741, "dot": -0.1947, "eos": 0.21577, "eth": 0.04397, "eur": 0.32703, "gbp": 0.23638, "hkd": 0.12371, "huf": 0.38063, "idr": 0.1057, "ils": 0.19151, "inr": 0.03549, "jpy": 0.23162, "krw": 0.16932, "kwd": 0.15147, "lkr": 0.11825, "ltc": 0.03849, "mmk": 0.11825, "mxn": 0.219, "myr": 0.16583, "ngn": 0.11825, "nok": 0.21591, "nzd": 0.27664, "php": 0.08213, "pkr": 0.11825, "pln": 0.35481, "rub": 0.94106, "sar": 0.12754, "sek": 0.18072, "sgd": 0.18511, "thb": 0.2718, "try": 0.15499, "twd": 0.08329, "uah": 0.11825, "usd": 0.11825, "vef": 0.11825, "vnd": 0.11825, "xag": 0.16498, "xau": 0.17426, "xdr": 0.11825, "xlm": 0.22374, "xrp": 0.0145, "yfi": -0.05459, "zar": 0.1868, "bits": 0.00952, "link": -0.14389, "sats": 0.00952}, "price_change_percentage_1y_in_currency": {"aed": -19.82674, "ars": -2.50543, "aud": -19.87537, "bch": 18.70046, "bdt": -18.03577, "bhd": -19.84179, "bmd": -19.82755, "bnb": -38.59258, "brl": -35.45612, "btc": 0.0, "cad": -20.47862, "chf": -21.20188, "clp": -12.90374, "cny": -22.31421, "czk": -19.67221, "dkk": -13.92778, "dot": 54.49749, "eos": 75.95536, "eth": -52.68358, "eur": -13.91379, "gbp": -15.53, "hkd": -19.24588, "huf": -11.70331, "idr": -20.7617, "ils": -22.8349, "inr": -17.72791, "jpy": -10.95749, "krw": -13.87965, "kwd": -19.20457, "lkr": 19.14876, "ltc": 28.64362, "mmk": 1.63561, "mxn": -21.75473, "myr": -18.46229, "ngn": -12.31582, "nok": -18.08487, "nzd": -19.43384, "php": -15.36948, "pkr": -2.99971, "pln": -13.43443, "rub": -11.35959, "sar": -19.8044, "sek": -13.70595, "sgd": -19.22202, "thb": -14.30162, "try": 44.13634, "twd": -19.61971, "uah": -14.88429, "usd": -19.82755, "vef": -19.82755, "vnd": -20.33022, "xag": -18.824, "xau": -28.16805, "xdr": -18.16058, "xlm": 49.59032, "xrp": -38.74973, "yfi": 26.33066, "zar": -20.41996, "bits": -0.04504, "link": 42.52934, "sats": -0.04504}, "price_change_percentage_7d_in_currency": {"aed": -0.66524, "ars": 0.13692, "aud": -2.29762, "bch": -3.00564, "bdt": -0.39384, "bhd": -0.66056, "bmd": -0.66267, "bnb": -6.33521, "brl": -4.21252, "btc": 0.0, "cad": -1.1088, "chf": -1.44999, "clp": -0.55076, "cny": -0.79362, "czk": -1.33878, "dkk": -0.3547, "dot": -4.79908, "eos": -0.40872, "eth": -6.11071, "eur": -0.3176, "gbp": -0.82783, "hkd": -0.61445, "huf": -1.47038, "idr": -0.71765, "ils": -1.1667, "inr": -1.62456, "jpy": -1.19573, "krw": -1.48993, "kwd": -0.6382, "lkr": 3.06767, "ltc": 0.52965, "mmk": -0.41372, "mxn": -2.01028, "myr": -0.80403, "ngn": -0.41238, "nok": -0.51493, "nzd": -1.91735, "php": -2.23531, "pkr": 0.71884, "pln": -1.74473, "rub": -14.32162, "sar": -0.66095, "sek": -1.64062, "sgd": -1.00164, "thb": -1.63422, "try": -1.35281, "twd": -1.29313, "uah": -0.81169, "usd": -0.66267, "vef": -0.66267, "vnd": -0.66205, "xag": 0.52845, "xau": -0.871, "xdr": -0.84582, "xlm": -0.84872, "xrp": 4.01281, "yfi": -7.85542, "zar": -1.50056, "bits": 0.07945, "link": -5.34317, "sats": 0.07945}, "price_change_percentage_14d_in_currency": {"aed": 13.69084, "ars": 15.35678, "aud": 10.28875, "bch": 0.85342, "bdt": 14.20202, "bhd": 13.69701, "bmd": 13.69068, "bnb": -1.12255, "brl": 5.83261, "btc": 0.0, "cad": 12.55289, "chf": 12.74914, "clp": 11.36585, "cny": 13.8302, "czk": 12.69067, "dkk": 14.27663, "dot": -6.66545, "eos": -6.47485, "eth": -6.4614, "eur": 14.31968, "gbp": 14.07513, "hkd": 13.80255, "huf": 13.51033, "idr": 13.80166, "ils": 13.31745, "inr": 12.1867, "jpy": 16.83496, "krw": 13.10649, "kwd": 13.91438, "lkr": 20.00029, "ltc": 4.51222, "mmk": 13.93912, "mxn": 10.57374, "myr": 13.83938, "ngn": 13.92468, "nok": 13.39853, "nzd": 11.79213, "php": 11.16508, "pkr": 15.81186, "pln": 12.69236, "rub": -9.89494, "sar": 13.68707, "sek": 12.45916, "sgd": 13.50026, "thb": 13.2528, "try": 12.88712, "twd": 14.46903, "uah": 13.9369, "usd": 13.69068, "vef": 13.69068, "vnd": 13.7174, "xag": 16.45781, "xau": 14.16173, "xdr": 14.05715, "xlm": 0.40397, "xrp": 14.95946, "yfi": -3.41042, "zar": 10.49928, "bits": 0.01613, "link": -1.4606, "sats": 0.01613}, "price_change_percentage_24h_in_currency": {"aed": 1.56615, "ars": 1.98458, "aud": 0.01205, "bch": -0.23849, "bdt": 1.62615, "bhd": 1.54619, "bmd": 1.56504, "bnb": -1.16948, "brl": 0.1807, "btc": 0.0, "cad": 1.24662, "chf": 1.44335, "clp": 1.05008, "cny": 1.57143, "czk": 2.04442, "dkk": 1.99407, "dot": -0.49044, "eos": -1.43291, "eth": -0.16663, "eur": 2.00444, "gbp": 1.44562, "hkd": 1.53393, "huf": 2.38247, "idr": 1.64718, "ils": 1.39239, "inr": 1.27507, "jpy": 1.73325, "krw": 1.3668, "kwd": 1.6348, "lkr": 0.75412, "ltc": 0.55219, "mmk": 1.6226, "mxn": 1.57265, "myr": 1.39647, "ngn": 1.8578, "nok": 1.37093, "nzd": 0.56318, "php": 1.35347, "pkr": 1.62389, "pln": 1.70859, "rub": 0.93097, "sar": 1.56951, "sek": 1.12979, "sgd": 1.46216, "thb": 1.31456, "try": 1.64582, "twd": 1.46223, "uah": 1.62296, "usd": 1.56504, "vef": 1.56504, "vnd": 1.67021, "xag": 1.60007, "xau": 1.52198, "xdr": 1.67649, "xlm": 2.50709, "xrp": 1.48591, "yfi": 1.63004, "zar": 0.80665, "bits": -0.05218, "link": 2.16535, "sats": -0.05218}, "price_change_percentage_30d_in_currency": {"aed": 18.29328, "ars": 22.08459, "aud": 14.38576, "bch": -7.80695, "bdt": 18.1076, "bhd": 18.28034, "bmd": 18.29634, "bnb": -0.00757, "brl": 7.36093, "btc": 0.0, "cad": 15.8127, "chf": 19.55953, "clp": 14.17364, "cny": 19.15954, "czk": 11.62515, "dkk": 17.99969, "dot": -10.43172, "eos": -15.20028, "eth": -10.39906, "eur": 17.96232, "gbp": 19.23207, "hkd": 18.57746, "huf": 12.87531, "idr": 18.07222, "ils": 15.54863, "inr": 16.57035, "jpy": 26.65688, "krw": 17.88724, "kwd": 18.68062, "lkr": 72.69141, "ltc": -1.63096, "mmk": 17.99884, "mxn": 11.96134, "myr": 19.21655, "ngn": 17.86011, "nok": 15.18668, "nzd": 16.05307, "php": 16.71489, "pkr": 22.12115, "pln": 11.48072, "rub": -4.22415, "sar": 18.27761, "sek": 13.02125, "sgd": 17.90724, "thb": 21.01631, "try": 22.76669, "twd": 20.32491, "uah": 15.44646, "usd": 18.29634, "vef": 18.29634, "vnd": 18.44586, "xag": 23.38429, "xau": 20.90733, "xdr": 18.54813, "xlm": -7.44525, "xrp": 7.96783, "yfi": 0.14683, "zar": 11.27758, "bits": -0.02873, "link": -4.25415, "sats": -0.02873}, "price_change_percentage_60d_in_currency": {"aed": 25.82701, "ars": 33.34859, "aud": 17.89397, "bch": -6.77216, "bdt": 26.59951, "bhd": 25.82684, "bmd": 25.82684, "bnb": 2.17507, "brl": 9.41868, "btc": 0.0, "cad": 23.69409, "chf": 26.60962, "clp": 19.89275, "cny": 25.88223, "czk": 31.39148, "dkk": 31.24986, "dot": 3.04154, "eos": 1.36827, "eth": -4.90568, "eur": 31.31209, "gbp": 30.38477, "hkd": 26.46629, "huf": 37.40738, "idr": 25.52474, "ils": 26.47705, "inr": 26.92886, "jpy": 34.5485, "krw": 27.18487, "kwd": 26.70687, "lkr": 83.60608, "ltc": 9.75091, "mmk": 26.245, "mxn": 21.2796, "myr": 26.63892, "ngn": 26.10678, "nok": 25.75, "nzd": 19.91514, "php": 26.60617, "pkr": 32.18389, "pln": 33.89919, "rub": 39.14274, "sar": 25.82456, "sek": 29.8332, "sgd": 26.943, "thb": 27.14472, "try": 36.47301, "twd": 29.75727, "uah": 31.27008, "usd": 25.82684, "vef": 25.82684, "vnd": 27.05065, "xag": 14.51003, "xau": 17.83401, "xdr": 26.68427, "xlm": 6.15477, "xrp": -7.65734, "yfi": 23.1009, "zar": 19.86715, "bits": -0.04262, "link": 17.36517, "sats": -0.04262}, "price_change_percentage_200d_in_currency": {"aed": -2.50315, "ars": 10.68918, "aud": -6.73561, "bch": 65.26983, "bdt": -1.11294, "bhd": -2.47883, "bmd": -2.49797, "bnb": -9.08986, "brl": -14.82081, "btc": 0.0, "cad": -4.18462, "chf": -2.63317, "clp": -3.03658, "cny": -3.91424, "czk": 0.64341, "dkk": 4.70816, "dot": 51.36689, "eos": 71.63334, "eth": -1.0569, "eur": 4.68607, "gbp": 2.49693, "hkd": -1.86906, "huf": 10.64612, "idr": -1.75787, "ils": -2.66402, "inr": -0.20113, "jpy": 9.26447, "krw": 0.71671, "kwd": -1.30823, "lkr": 44.47796, "ltc": 43.51085, "mmk": -4.92483, "mxn": -3.06815, "myr": -1.26926, "ngn": -1.36731, "nok": -1.55761, "nzd": -1.46281, "php": 0.03787, "pkr": 7.15153, "pln": 5.99073, "rub": 13.6617, "sar": -2.47561, "sek": 6.18957, "sgd": -1.73673, "thb": -1.57431, "try": 68.09939, "twd": 0.55239, "uah": 7.61424, "usd": -2.49797, "vef": -2.49797, "vnd": -2.00624, "xag": -9.22974, "xau": -11.28964, "xdr": -0.24179, "xlm": 40.27683, "xrp": 28.40196, "yfi": 41.71657, "zar": -2.95333, "bits": 0.01955, "link": 68.54046, "sats": 0.01955}, "market_cap_change_percentage_24h_in_currency": {"aed": 1.75548, "ars": 2.17753, "aud": 0.23362, "bch": -0.23846, "bdt": 1.81559, "bhd": 1.73359, "bmd": 1.75437, "bnb": -0.92671, "brl": 0.37606, "btc": 0.00543, "cad": 1.46912, "chf": 1.61667, "clp": 1.23845, "cny": 1.76077, "czk": 2.19627, "dkk": 2.14773, "dot": -0.60681, "eos": -1.29596, "eth": -0.10683, "eur": 2.15738, "gbp": 1.62154, "hkd": 1.72158, "huf": 2.52911, "idr": 1.83838, "ils": 1.57412, "inr": 1.41483, "jpy": 1.90317, "krw": 1.55576, "kwd": 1.83095, "lkr": 0.94193, "ltc": 0.60037, "mmk": 1.81203, "mxn": 1.76634, "myr": 1.58548, "ngn": 2.04768, "nok": 1.47637, "nzd": 0.77479, "php": 1.56909, "pkr": 1.81332, "pln": 1.79051, "rub": 1.3312, "sar": 1.75855, "sek": 1.29576, "sgd": 1.65879, "thb": 1.50165, "try": 1.85742, "twd": 1.65137, "uah": 1.81239, "usd": 1.75437, "vef": 1.75437, "vnd": 1.85973, "xag": 1.8163, "xau": 1.69555, "xdr": 1.86602, "xlm": 2.46289, "xrp": 1.43038, "yfi": 1.59723, "zar": 0.98354, "bits": -0.01717, "link": 2.2505, "sats": -0.01717}}', 'Bitcoin', '{"": ""}', 0, '{"alexa_rank": 9440, "bing_matches": null}', null, 21.86, 78.14, '[]', 'btc', null, null)
on conflict do nothing;
