from datetime import datetime, timezone, timedelta

from smartcitizen_connector.models import (Check, Export, HardwarePostprocessing,
                                           Policy, Postprocessing)
from smartcitizen_connector.models.models import HardwareVersion


def test_hardware_version_reads_from_and_to_keys():
    version = HardwareVersion.model_validate(
        {'ids': {'AS_48_32': '212830246'}, 'from': '2024-04-01', 'to': '2025-01-01T10:00:00'})

    assert version.from_date == datetime(2024, 4, 1, tzinfo=timezone.utc)
    assert version.to_date == datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    assert version.ids == {'AS_48_32': '212830246'}


def test_hardware_version_accepts_field_names():
    version = HardwareVersion(from_date=datetime(2024, 4, 1))

    assert version.from_date == datetime(2024, 4, 1, tzinfo=timezone.utc)
    assert version.to_date is None


def test_hardware_version_keeps_aware_dates():
    cest = timezone(timedelta(hours=2))
    version = HardwareVersion.model_validate({'from': datetime(2024, 4, 1, tzinfo=cest)})

    assert version.from_date.utcoffset() == timedelta(hours=2)


def test_hardware_postprocessing(hardware):
    hardware_postprocessing = HardwarePostprocessing.model_validate(hardware)

    assert hardware_postprocessing.blueprint_url.endswith('sc_air.json')
    assert len(hardware_postprocessing.versions) == 1
    assert hardware_postprocessing.versions[0].from_date == datetime(2024, 4, 1, tzinfo=timezone.utc)
    assert hardware_postprocessing.versions[0].to_date is None


def test_postprocessing_allows_empty_fields():
    postprocessing = Postprocessing.model_validate({'device_id': 1, 'hardware_url': 'SCAS220013'})

    assert postprocessing.blueprint_url is None
    assert postprocessing.latest_postprocessing is None


def test_check_and_export_defaults():
    check = Check.model_validate({'name': 'GAPS', 'function': 'find_gaps'})
    export = Export.model_validate({'name': 'all'})

    assert check.module == 'scdata.device.check'
    assert check.store_qc is True
    assert check.clean is False
    assert export.columns == []


def test_policy_allows_null_forwarding_destination():
    policy = Policy.model_validate({'is_private': False, 'precise_location': True,
                                    'enable_forwarding': False, 'forwarding_destination': None})

    assert policy.forwarding_destination is None
