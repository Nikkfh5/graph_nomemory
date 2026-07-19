# Graph NoMemory

Вычисление PageRank для большого ориентированного графа на одном Linux-узле при жёстком ограничении оперативной памяти. Граф хранится и обрабатывается на диске; в RAM остаются два массива rank, степени исхода и ограниченные буферы.

## Сборка

Требуются CMake 3.22+, компилятор C++20, Make и POSIX Threads.

```bash
git clone https://github.com/Nikkfh5/graph_nomemory.git
cd graph_nomemory
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Сборка создаёт один публичный executable: `build/tbank`.

## Запуск: CSV → CSV

```bash
printf 'from,to\n1,2\n2,3\n3,1\n4,1\n' > edges.csv
build/tbank edges.csv pagerank.csv
cat pagerank.csv
```

Публичный интерфейс:

```text
tbank INPUT.csv OUTPUT.csv [CONFIG]
```

`INPUT.csv` содержит заголовок `from,to` и ориентированные рёбра с идентификаторами `int32`. `OUTPUT.csv` создаётся программой и содержит `vertex,rank`; существующий файл не перезаписывается. Необязательный `CONFIG` задаёт строгие overrides поверх встроенных defaults.

## Синтетический вход

```bash
python3 tools/generators/generate.py \
  --profile reduced-skew \
  --output ./dataset
build/tbank dataset/edges.csv pagerank.csv
```

Генератор создаёт `edges.csv` и воспроизводимый `manifest.json`. Профили и проверка результата описаны в [`tools/generators/README.md`](tools/generators/README.md).

## Документация

- [Запуск, config v1 и коды завершения](docs/configuration.md)
- [Алгоритм, сложность и гарантии](docs/algorithm.md)
