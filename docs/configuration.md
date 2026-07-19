# Запуск и конфигурация `main`

## Публичный контракт

```text
main INPUT.csv OUTPUT.csv
```

- `INPUT.csv` — существующий regular file с ориентированными рёбрами;
  symlink в последнем компоненте path запрещён.
- `OUTPUT.csv` — ещё не существующий path результата `vertex,rank` внутри
  существующего real directory. Файл публикуется atomically и не
  перезаписывается.
- `main --help` печатает synopsis и ссылку на этот документ.

Путь config фиксирован: `./config/main.conf` относительно current working
directory процесса. В CLI он не передаётся. Программа не читает environment
overrides и не ищет system или user config.

Если `./config/main.conf` отсутствует (`ENOENT`), используются встроенные
defaults. Существующий, но unreadable, malformed, symlink в последнем
компоненте или non-regular config является явной ошибкой: fallback в этом
случае запрещён. В Docker рабочая директория — `/data`, а образ содержит
`/data/config/main.conf`.

Input/output paths через config не задаются.

## Формат config v1

Tracked [`config/main.conf`](../config/main.conf) содержит все 35 изменяемых
defaults и автоматически применяется при запуске из корня checkout. Чтобы
переопределить параметр, измените его value в этом файле. Config —
dependency-free ASCII `key=value` document размером не более `65536` bytes:

```text
# values may be edited
schema=tbank-run-config-v1
parallel.worker_count=1
pagerank.max_iterations=300
```

Правила:

- файл непустой и заканчивается LF;
- разрешены printable ASCII bytes и LF; CR, NUL и non-ASCII запрещены;
- пустые строки и full-line comments, начинающиеся с `#`, игнорируются;
- первая semantic-строка должна в точности быть
  `schema=tbank-run-config-v1`;
- каждая следующая semantic-строка содержит ровно один `=` и непустые key и
  value; пробелы не удаляются;
- unknown key, duplicate key и повторный `schema` являются ошибкой;
- порядок различных overrides не меняет resolved config;
- integers записываются canonical unsigned decimal: `0` либо ненулевая цифра
  и последующие digits; знак, ведущие нули, whitespace и overflow запрещены;
- floating values используют canonical finite decimal grammar; domain
  constraints проверяются после полного overlay.

Config и resource preflight валидируются до создания временных данных или
output. Invalid config возвращает ненулевой exit и не меняет filesystem.

## Встроенные defaults

### Preprocessing

| Key | Default |
| --- | ---: |
| `preprocess.non_bulk_reserve_bytes` | `33554432` |
| `preprocess.input_chunk_bytes` | `1048576` |
| `preprocess.phase_fd_budget` | `64` |
| `preprocess.disk_reserve_bytes` | `1073741824` |
| `preprocess.edge_slice_size` | `8192` |
| `preprocess.max_task_edges` | `262144` |
| `preprocess.max_task_vertices` | `4096` |
| `preprocess.endpoint_ids_per_run` | `1048576` |
| `preprocess.run_writer_buffer_bytes` | `65536` |
| `preprocess.vertex_merge_fan_in` | `32` |
| `preprocess.vertex_merge_reader_buffer_bytes` | `65536` |
| `preprocess.vertex_merge_writer_buffer_bytes` | `65536` |
| `preprocess.vertex_merge_crc_chunk_bytes` | `65536` |
| `preprocess.raw_edges_per_run` | `1048576` |
| `preprocess.compact_reader_buffer_bytes` | `65536` |
| `preprocess.compact_writer_buffer_bytes` | `65536` |
| `preprocess.compact_crc_chunk_bytes` | `65536` |
| `preprocess.edge_merge_fan_in` | `32` |
| `preprocess.edge_merge_reader_buffer_bytes` | `65536` |
| `preprocess.edge_merge_writer_buffer_bytes` | `65536` |
| `preprocess.edge_merge_crc_chunk_bytes` | `65536` |
| `preprocess.edge_batch_records` | `8192` |
| `preprocess.graph_validation_chunk_bytes` | `65536` |

### Analytics и parallel execution

| Key | Default |
| --- | ---: |
| `analyze.record_batch_records` | `8192` |
| `analyze.validation_chunk_bytes` | `65536` |
| `analyze.runtime_reserve_bytes` | `33554432` |
| `parallel.worker_count` | `64` |
| `parallel.worker_stack_bytes` | `1048576` |
| `parallel.worker_guard_bytes` | `4096` |
| `parallel.worker_count_batch_records` | `8192` |
| `parallel.worker_source_batch_records` | `8192` |
| `parallel.scheduler_window_records` | `64` |

Main-thread stack не является user claim: executable читает фактический finite
soft `RLIMIT_STACK` и включает его в memory preflight. Unlimited stack
отклоняется. Для sequential режима нужно одновременно задать
`parallel.worker_count=0` и остальные пять `parallel.*` resource fields в
`0`; неполная комбинация отклоняется.

### PageRank

| Key | Default |
| --- | ---: |
| `pagerank.alpha` | `0.85` |
| `pagerank.eta` | `1e-8` |
| `pagerank.max_iterations` | `200` |

`alpha` должен быть finite и находиться в `(0,1)`, `eta` — finite и
положительным, `max_iterations` — положительным. Buffer, stack, fan-in, task и
reserve values проходят checked resource planners; конфликтующие или
непредставимые комбинации отклоняются до запуска pipeline.

## Ограничение 128 MiB

`memory_budget_bytes` пока не является гиперпараметром. Preprocessing и
PageRank реально enforce hard `134217728` bytes (`128 MiB`), а численные
эксперименты покрывают default domain `V<=1,000,000`. Декоративный config key
создал бы ложную конфигурируемость: изменение budget требует параметризации
обоих resource planners и новых проверок. Config v1 оставляет эту работу
отдельным follow-up.

## Временные данные и публикация

После preflight `main` создаёт собственный каталог `.tbank-run-*` рядом с
output, выполняет preprocessing и PageRank, затем удаляет только этот каталог.
Обычная ошибка очищает intermediates и не публикует output. Crash может
оставить отличимый `.tbank-run-*`; он не считается завершённым результатом.

Если финальный parent `fsync` после rename завершился ошибкой, output может
быть видим, но процесс возвращает отдельный durability-uncertain status и не
пытается удалить уже опубликованный файл.

## Выход process и ошибки

Успешный запуск quiet: результат находится в `OUTPUT.csv`. Diagnostic пишется
в stderr одной человекочитаемой строкой.

| Exit | Класс |
| ---: | --- |
| `0` | success |
| `2` | invocation/config usage |
| `3` | input/data/format |
| `4` | resource preflight/allocation |
| `5` | non-convergence |
| `6` | numerical verification |
| `7` | publication/no-replace |
| `8` | durability uncertain after publication |
| `9` | OS/I/O system error |
| `10` | internal invariant/error |

Формат входа и выхода кратко описан в [`README.md`](../README.md), алгоритм и
resource domain — в [`algorithm.md`](algorithm.md), экспериментальная область
— в [`research.md`](../research.md).
