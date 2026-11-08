# XMLRiver MCP Toolkit

Независимый, неофициальный Python-пакет для XMLRiver. Собственные ключи пользователя,
stdio MCP, асинхронный клиент и CLI. MIT, Python 3.11+.

Имя пакета PyPI: `xmlriver-mcp-toolkit`. Команды: `xmlriver-mcp` и `xmlriver`.
Релиз 0.1.0 готовится; факт публикации нужно подтвердить отдельно.

```sh
uvx --from xmlriver-mcp-toolkit==0.1.0 xmlriver-mcp
xmlriver balance
xmlriver wordstat top "кофе" --limit 50
xmlriver serp "кофе" --pages 5
xmlriver positions run --config examples/positions.yaml --dry-run
```

Ключи передаются через `XMLRIVER_USER` и `XMLRIVER_KEY`. Пакет не читает `.mcp.json`.
Четыре MCP-инструмента: `get_balance`, `wordstat_top`, `wordstat_history`, `serp`.
Примеры Claude Code, Claude Desktop, Codex и Cursor приведены в README.md.
Интерактивная проверка этих клиентов пока не выполнена.

Запросы и повторы могут списывать деньги. `http_attempts` считает HTTP-попытки,
а не подтверждённые списания. Неизвестная стоимость возвращается как null.
`--dry-run` ничего не записывает и не обращается к сети. Команда `balance`
обращается к сети; региональная проверка Яндекса платная и эвристическая.

Кэш учитывает глубину, домен, поддомены, устройство и регион. TTL 24 часа.
Результаты по умолчанию сохраняются в `xmlriver-data` рядом с YAML.
CSV-пути в YAML разрешаются относительно YAML. Для отдельного хранилища есть
`--workspace`. Перед обновлением CSV создаётся уникальная резервная копия.
Даты берутся из фактически полученных результатов, включая кэшированные.

Подробные ограничения, формат конфигурации и API находятся в README.md и examples/.
