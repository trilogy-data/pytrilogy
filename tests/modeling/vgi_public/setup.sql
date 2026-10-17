INSTALL vgi FROM community;
LOAD vgi;

ATTACH IF NOT EXISTS 'https://vgi-open-meteo.rusty-bb6.workers.dev' AS open_meteo (TYPE vgi);
ATTACH IF NOT EXISTS 'https://vgi-earthquakes.rusty-bb6.workers.dev' AS earthquakes (TYPE vgi);
ATTACH IF NOT EXISTS 'https://vgi-trains-ts.rusty-bb6.workers.dev' AS trains (TYPE vgi);
ATTACH IF NOT EXISTS 'https://vgi-overture.rusty-bb6.workers.dev' AS overture (TYPE vgi);
