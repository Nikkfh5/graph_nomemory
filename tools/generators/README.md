# Генератор синтетических графов

Генератор создаёт детерминированный edge CSV для фиксированных тестовых профилей. Произвольные количество вершин, количество рёбер и плотность сейчас не поддерживаются.

## Быстрый запуск

Из корня чистого checkout:

```bash
python3 tools/generators/generate.py \
  --profile reduced-skew \
  --output ./dataset
```

Каталог `./dataset` не должен существовать. Результат:

- `edges.csv` — готовый ориентированный граф с заголовком `from,to`;
- `manifest.json` — профиль, размеры, SHA-256 и Git revision генератора.

Git `HEAD` определяется автоматически. Для локального запуска с изменёнными tracked-файлами добавьте `--allow-dirty`; manifest будет явно помечен как dirty.

## Профили

| Профиль | Вершины | Рёбра |
| --- | ---: | ---: |
| `reduced-skew` | 101 | 404 |
| `scale` | 1 000 000 | 40 000 000 |
| `skew` | 1 000 000 | 50 000 000 |
| `scenario-b` | 10 000 000 | 40 000 000 |

## Проверка результата

```bash
python3 tools/generators/generate.py --verify ./dataset
```

Проверка формул без создания файлов:

```bash
python3 tools/generators/generate.py \
  --profile reduced-skew \
  --validate-only
```

Остальные флаги: `python3 tools/generators/generate.py --help`.
