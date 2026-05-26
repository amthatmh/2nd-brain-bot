def test_telegram_error_location_names_crossfit_notion_save():
    from second_brain.error_reporting import telegram_error_location

    namespace = {}
    code = compile(
        "def create_wod_log():\n    raise RuntimeError('boom')\n",
        "/tmp/app/second_brain/crossfit/notion.py",
        "exec",
    )
    exec(code, namespace)

    try:
        namespace["create_wod_log"]()
    except RuntimeError as exc:
        location = telegram_error_location(exc)

    assert location == "CrossFit Notion save (second_brain.crossfit.notion.create_wod_log)"
