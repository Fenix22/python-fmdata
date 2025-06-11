# fmdata

`fmdata` is a small wrapper around the [FileMaker Data API](https://help.claris.com/en/data-api/). It exposes the REST endpoints via a simple `FMClient` and optionally provides an ORM style interface built on top of `marshmallow`.

## Installation

Install the library from PyPI:

```bash
pip install fmdata
```

For Claris Cloud OAuth support install with the extra dependencies:

```bash
pip install fmdata[cloud]
```

## Basic usage (without the ORM)

```python
from fmdata import FMClient
from fmdata.session_providers import UsernamePasswordSessionProvider

client = FMClient(
    url="https://filemaker.example.com",
    database="MyDatabase",
    login_provider=UsernamePasswordSessionProvider(
        username="account",
        password="secret",
    ),
)

# create a record
create = client.create_record(
    layout="Contacts",
    field_data={"Name": "Alice"}
)
create.raise_exception_if_has_error()
record_id = create.response.record_id

# find records
result = client.find(
    layout="Contacts",
    query=[{"Name": "Alice"}],
    limit=10,
)
for entry in result.response.data:
    print(entry.record_id, entry.field_data)

# read a single record
record = client.get_record(layout="Contacts", record_id=record_id)
print(record.response.data[0].field_data)

# update
client.edit_record(
    layout="Contacts",
    record_id=record_id,
    field_data={"Name": "Alice Smith"},
)

# delete
client.delete_record(layout="Contacts", record_id=record_id)
```

### Portal CRUD with `FMClient`

```python
# add a related record
client.edit_record(
    layout="Contacts",
    record_id=record_id,
    field_data={},
    portal_data={"Phones": [{"Number": "123-456"}]},
)

# fetch portal data
record = client.get_record(
    layout="Contacts",
    record_id=record_id,
    portals={"Phones": {"limit": 10}},
)
phone = record.response.data[0].portal_data["Phones"][0]
print(phone.record_id, phone.fields)

# update the portal row
client.edit_record(
    layout="Contacts",
    record_id=record_id,
    field_data={},
    portal_data={"Phones": [{"recordId": phone.record_id, "Number": "987-654"}]},
)

# delete the portal row
client.edit_record(
    layout="Contacts",
    record_id=record_id,
    field_data={"deleteRelated": f"Phones.{phone.record_id}"},
)
```

## Using the ORM

```python
from fmdata.orm import Model, PortalField, PortalModel
from marshmallow import fields

class Phone(PortalModel):
    number = fields.Str(data_key="Phones::Number")

class Contact(Model):
    class Meta:
        client = client
        layout = "Contacts"

    record_id = fields.Str(data_key="PrimaryKey")
    name = fields.Str(data_key="Name")
    phones = PortalField(model=Phone, name="Phones")

# create
contact = Contact.objects.create(name="Alice")

# query
for item in Contact.objects.find(name__exact="Alice"):
    print(item.record_id, item.name)

# update
contact.name = "Alice Smith"
contact.save()

# delete
contact.delete()

# portal CRUD
contact.phones.create(number="123-456")
phone = contact.phones.first()
phone.number = "987-654"
phone.save()
phone.delete()

```

The ORM layer gives you a Django style API with query sets and automatic conversion of fields via `marshmallow`. Portal records are also supported through `PortalField`.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
