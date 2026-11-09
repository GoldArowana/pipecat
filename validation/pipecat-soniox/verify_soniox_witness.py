"""Validate the one expected regression failure and its structured observation."""

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def main():
    log_path, junit_path = (Path(name) for name in sys.argv[1:])
    prefix = "SONIOX_RECONNECT_WITNESS "
    records = [
        json.loads(line[len(prefix):])
        for line in log_path.read_text().splitlines()
        if line.startswith(prefix)
    ]
    expected = {
        "reconnect": True,
        "active_interruptible": False,
        "new_word_before_old_release": False,
        "original": [],
        "replacement": [
            {"type": "agent_text", "text": "old active", "end_of_segment": False},
            {"type": "agent_text", "text": " new due", "end_of_segment": False},
            {"type": "agent_text", "text": "", "end_of_segment": True},
        ],
    }
    assert records == [expected], records
    cases = ET.parse(junit_path).findall(".//testcase")
    assert len(cases) == 1, [case.attrib for case in cases]
    assert cases[0].get("name") == (
        "test_reconnect_retires_in_flight_agent_text[active-uninterruptible-reconnect]"
    )
    failure = cases[0].find("failure")
    assert failure is not None and "soniox_connection_generation_contract" in failure.attrib.get("message", ""), (
        None if failure is None else failure.attrib
    )
    assert cases[0].find("error") is None and cases[0].find("skipped") is None
    print("Exact expected Soniox reconnect regression reproduced by installed Pipecat package")


if __name__ == "__main__":
    main()
