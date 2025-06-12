# A POC/demo of a streaming app through to a web viz.

NYC Yellow Taxi trip dataset --> Kafka --> Materialize --> Java client app --> Web UI (chart.js)

# Raw notes

## How it got written

Google gemini code assist for initial prototyping and high level stuff
VSCode + Github Copilot for further tweaks/edits

## TODO
 aside from it just being raw demo code done in "a day"...
 order the nyc input data, the parquet file isn't in time order? so the demo can be a "playback of real time"
 a control ui for the input data (pause/reset/speed)
 better final dash and better handling of the data streaming to chartjs
 something better than chartjs?

## Stuff I wished for
 higher-level API (Java) so I could "register" queries and receive streaming data (snapshot/updates) in a single spot (vs threading sql cursor queries)
 
