# Stream Directory — Stage 3

Place the 12 micro-batch JSONL files here for local Stage 3 testing:

    stream/
      stream_20260320_143000_0001.jsonl
      stream_20260320_143500_0002.jsonl
      ...

JSONL files are excluded via .gitignore — only .gitkeep and this README
should be committed.

At runtime the scoring system mounts the stream directory at /data/stream/.
