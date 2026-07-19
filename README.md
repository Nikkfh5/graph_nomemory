# Graph NoMemory

Детерминированное вычисление стандартного PageRank для большого невзвешенного ориентированного графа на одном Linux x86-64 узле при жёстком ограничении `128 MiB` оперативной памяти. Вход сортируется и преобразуется ограниченными порциями, рёбра и каталог задач остаются на диске, а в RAM находятся два массива rank, степени исхода, явно учтённые стеки потоков и ограниченные буферы. Входящие рёбра гиперузлов делятся на bounded slices, поэтому степень одной вершины не задаёт размер рабочего буфера.

PageRank выбран как глобальная метрика, вычисляемая повторными потоковыми проходами по дисковому представлению. Результат публикуется только после отдельного полного residual pass, проверки массы и finite rank values; случайное зерно и зависящие от scheduler floating-point reductions не используются.

Архитектура semi-external: объём рёбер может превышать RAM, но состояние вершин размером `20V` bytes вместе со стеками, буферами и reserve должно помещаться в бюджет. Если этот preflight не проходит, программа завершается до анализа; автоматического fully-external fallback нет.

## Сборка

Требуются CMake 3.22+, компилятор C++20, Make и POSIX Threads. Сборка проверена на Ubuntu 22.04 x86-64.

```bash
git clone https://github.com/Nikkfh5/graph_nomemory.git
cd graph_nomemory
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Сборка создаёт один публичный executable: `build/main`.

## Запуск: CSV → CSV

```bash
printf 'from,to\n1,2\n2,3\n3,1\n4,1\n' > edges.csv
build/main edges.csv pagerank.csv
cat pagerank.csv
```

Публичный интерфейс:

```text
main INPUT.csv OUTPUT.csv
```

`INPUT.csv` — UTF-8 без BOM: точный заголовок `from,to`, затем одно ребро
`from -> to` на строку. ID — канонический `int32` (`0|-?[1-9][0-9]*`); знак
`+`, `-0`, ведущие нули, кавычки, пустые и лишние поля запрещены. Дубликаты
рёбер удаляются, петли сохраняются. Множество вершин образуют концы рёбер;
изолированные вершины задать нельзя.
`OUTPUT.csv` создаётся программой и содержит `vertex,rank`; существующий файл
не перезаписывается.

Config лежит в [`config/main.conf`](config/main.conf). `main` автоматически
читает только `./config/main.conf` относительно текущей директории; путь в CLI
не передаётся. Файл содержит все изменяемые defaults — редактируйте нужные
значения прямо в нём. Если файла нет, используются те же встроенные defaults.

```bash
build/main edges.csv pagerank.csv
```

Формат, ограничения и смысл всех ключей описаны в
[`docs/configuration.md`](docs/configuration.md).

## Многопоточность и память

Основной параллельный участок — обработка задач внутри каждого destination-pull прохода PageRank по дисковому графу. Подготовка CSV в disk-backed layout, полная проверка этого layout, векторные редукции и публикация итогового CSV остаются последовательными. Persistent pthread pool получает независимые задачи без неограниченной очереди; разные задачи записывают rank разных вершин, а partial sums гиперузла объединяются координатором в фиксированном порядке. Поэтому число потоков не меняет порядок floating-point reductions: в проверочных запусках результаты одного и 64 потоков совпали побитово.

`cmake --build build --parallel` распараллеливает только сборку. Число runtime workers задаёт `parallel.worker_count`.

Встроенный профиль использует `parallel.worker_count=64`, потому что воспроизводимые измерения проводились на стенде с 64 доступными логическими CPU. Это фиксированный проверенный default, а не скрытое автоматическое определение hardware. На машине с меньшим числом доступных CPU укажите результат `nproc` явно через config, чтобы не создавать лишние workers; на более крупной машине пример оставляет проверенный предел `64`:

```bash
workers=$(nproc)
if [ "$workers" -gt 64 ]; then workers=64; fi
sed -i "s/^parallel.worker_count=.*/parallel.worker_count=$workers/" config/main.conf
build/main edges.csv pagerank.csv
```

Фактический параллелизм ограничен минимумом из числа workers, размера scheduler window и числа готовых задач, поэтому маленький граф не обязан загружать все CPU. Масштабирование выше 64 workers не измерялось; одного увеличения `worker_count` при default window `64` для него недостаточно. Выбранное число потоков, стеки, guard pages, per-worker I/O buffers, scheduler window, фактический main-thread stack и runtime reserve входят в checked memory preflight; запуск, не помещающийся в hard budget, отклоняется до основного edge pass. Назначение и происхождение parallel defaults описаны в [`docs/configuration.md`](docs/configuration.md#как-устроен-reference-parallel-profile).

На 64-CPU стенде работу выполняли `64/64` workers; медианное ускорение вычислительного ядра составило `17.73x`. Полный исторический этап анализа ускорился только в `1.15x`, поскольку validation и публикация CSV остаются последовательными/I/O-bound. Условия и границы измерения приведены в [`research.md`](research.md#измерение-производительности).

## Синтетический вход

```bash
python3 tools/generators/generate.py \
  --profile reduced-skew \
  --output ./dataset
build/main dataset/edges.csv pagerank.csv
```

Генератор создаёт `edges.csv` и воспроизводимый `manifest.json`. `reduced-skew` — быстрый функциональный пример, а не демонстрация загрузки всех workers. Профили `scale` (`1M/40M`) и `skew` (`1M/50M`, степень гиперузла `50000`), а также проверка результата описаны в [`tools/generators/README.md`](tools/generators/README.md); условия измерений — в [`research.md`](research.md#проверочные-графы).

## Документация

- [Запуск, config defaults, memory preflight и коды завершения](docs/configuration.md)
- [PageRank, рассмотренные способы, выбранный алгоритм, сложность, точность и ограничения](docs/algorithm.md)
- [Эксперименты: граф больше RAM, гиперузлы, многопоточность и воспроизводимость](research.md)
