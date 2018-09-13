# logdrain-transformer

A tool to be used as an endpont for a Heroku Logplex HTTPS Log Drain. Parses the syslogs, puts them into an acceptable format for indexing via Elasticsearch, and then sends them to a Bonsai Elasiticsearch add-on endpoint.

 - Stores the logs as a set of 3 fields: date, host, and message.
 - Expects an environment variable called BONSAI_URL for the Elastic endpoint (which is the default config var for a Heroku Bonsai add-on).
