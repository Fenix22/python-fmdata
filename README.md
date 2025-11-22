[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=Fenix22_python-fmdata&metric=coverage)](https://sonarcloud.io/summary/new_code?id=Fenix22_python-fmdata)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=Fenix22_python-fmdata&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=Fenix22_python-fmdata)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Fenix22_python-fmdata&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Fenix22_python-fmdata)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=Fenix22_python-fmdata&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=Fenix22_python-fmdata)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=Fenix22_python-fmdata&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=Fenix22_python-fmdata)

# fmdata - A Python ORM for the FileMaker Data API

**fmdata** is a lightweight and modern **Python ORM for the FileMaker Data API**,
providing Django-style models, type-safe field definitions, and Pythonic access to your **FileMaker Server / Claris
FileMaker** data.
It streamlines integration with the **FileMaker Data API** by replacing low-level HTTP requests with a familiar ORM
layer.

## Why this library exists

At first glance, FileMaker seems simple — and it is, until you start using it in a real project. Once you’re building a
real application or, worse, running a business on top of it, the rough edges start to show.

Working directly with the Data API quickly reveals a few challenges:

- You need **strong guarantees about the data you read and write**.  
  If you’re interacting with a **Number** field and expect an `Integer`, but the Data API returns a `Decimal` or even a
  `String`, you don’t want to discover that three screens later — or in your production logs. You want to know *
  *immediately**.  
  That means validating your data every time you talk to the API.

  > Yes, FileMaker Number fields can store strings — you knew that, right?

- FileMaker dates and timestamps are **US-formatted by default**.  
  Write `10/12/2024` intending 10 December, and FileMaker will happily interpret it as 12 October. That’s a silent
  data-corruption bug waiting to happen. You shouldn’t need to think about date formats on every API request.

- Portals introduce yet another level of complexity.  
  Retrieving the `record_id` of a newly created portal record isn’t straightforward. Creating, updating or deleting records in
  multiple portals within the same request (to simulate a transaction) is even trickier. Then there’s remembering **when
  to use the portal name** vs. the **table occurrence name**, how fields map to your models, and how they appear in API
  responses.  
  None of this is something you should have to reason about — or hand-code — for every request.

- The official documentation is sparse and incomplete. Many of the pitfalls only become visible too late, after your
  solution is already in use.

`fmdata` exists to provide that missing structure:

- Strongly-typed **fields** that validate what you send and what you receive.
- Automatic **conversion** between Python types and FileMaker formats (for example ISO dates vs US dates) so you do not
  have to ensure format rules everywhere.
- Clear, explicit **model and portal mapping**, so you always know whether you are talking about a layout, a portal
  name or a table occurrence.
- A Django-style **ORM** that encapsulates the complexity of Find/Omit, portals, chunking and pagination, so you can
  focus on your domain instead of on the quirks of the Data API.

## Installation

```bash
pip install fmdata
```

For Claris Cloud support:

```bash
pip install fmdata[cloud]
```

## Requirements

- Python 3.9+
- FileMaker Server 17+ with Data API enabled
- Valid FileMaker database with appropriate privileges

## Quick start

This section gives you a minimal, end-to-end example:

1. Create a client using a username/password session
2. Define a simple model (`Person`)
3. Run a few operations (find, create, update, delete)

```python
from datetime import date

import fmdata
from fmdata import FMFieldType, FMVersion, UsernamePasswordLogin
from fmdata.orm import Model

login_provider = UsernamePasswordLogin(
    username="your_username",
    password="your_password",
)

client = fmdata.Client(
    url="https://your-filemaker-server.com",
    database="your_database",  # database name without .fmp12
    version="22",  # or another supported version (17+)
    login_provider=login_provider,
)


class Person(Model):
    class Meta:
        client = client
        layout = "person"  # a layout based on your people table

    name = fmdata.String(field_name="Name", field_type=FMFieldType.Text)
    last_name = fmdata.String(field_name="LastName", field_type=FMFieldType.Text)
    birth_date = fmdata.Date(field_name="BirthDate", field_type=FMFieldType.Date)


# --- Query --------------------------------------------------------------
people = (
    Person
    .objects
    .find(name="Alice", birth_date__gt=date(1990, 1, 1))
    .order_by("-birth_date")[:10]
)

for person in people:
    print(person.name, person.last_name)

# --- Create -------------------------------------------------------------
john = Person.objects.create(
    name="John",
    last_name="Doe",
    birth_date=date(1990, 1, 1),
)

# --- Update -------------------------------------------------------------
john.last_name = "Smith"
john.save()

# --- Delete -------------------------------------------------------------
john.delete()
```

The rest of this document goes into more detail about the main concepts and
APIs.

## Connection and authentication

`fmdata` supports several authentication flows. The most common are:

- Username/password
- Username/password with additional data sources
- Claris Cloud

### Username / password

```python
import fmdata
from fmdata import FMVersion

session = fmdata.UsernamePasswordLogin(
    username="your_username",
    password="your_password",
)

client = fmdata.Client(
    url="https://your-filemaker-server.com",
    database="your_database",
    version="22",
    login_provider=session,
)
```

### Username / password with data sources

Use data sources when FileMaker scripts or calculated fields need to open
**other files** while serving your API requests.

```python
import fmdata
from fmdata import FMVersion

session = fmdata.UsernamePasswordLogin(
    username="your_username",
    password="your_password",
    data_sources=[
        fmdata.UsernamePasswordDataSource(
            database="AnotherDatabaseName",  # name of the target file without .fmp12 extension
            username="countries_username",
            password="countries_password",
        ),
    ],
)

client = fmdata.Client(
    url="https://your-filemaker-server.com",
    database="your_database",
    version="22",
    login_provider=session,
)
```

> **Important:** FileMaker does **not** raise an error during login if a data
> source has wrong credentials. Problems will only surface later, for example
> when a script silently fails to open the external file. Test your
> configuration carefully.

### Claris Cloud

```python
import fmdata
from fmdata import FMVersion

session = fmdata.ClarisCloudLogin(
    claris_id_name="your_claris_id_email",
    claris_id_password="your_claris_id_password",
)

client = fmdata.Client(
    url="https://your-filemaker-server.com",
    database="your_database",
    version="22",
    login_provider=session,
)
```

### Configuration Options

```python
fm_client = fmdata.Client(
    url="https://your-server.com",
    database="your_database",
    login_provider=session_provider,
    version="22",
    connection_timeout=10,  # Connection timeout in seconds
    read_timeout=30,  # Read timeout in seconds
    verify_ssl=True,  # SSL certificate verification
)
```

## Defining models and portals

`fmdata.orm` exposes a small ORM layer inspired by Django. You describe your
FileMaker layout, fields and portals using Python classes.

```python
import fmdata
from fmdata import FMFieldType

ADDRESS_PORTAL_TABLE_OCCURRENCE = "person_addresses"
ADDRESS_CITY_INFO_TABLE_OCCURRENCE = "address_city_info"


class AddressPortal(fmdata.PortalModel):
    class Meta:
        # Name of the **table occurrence** backing this portal in FileMaker
        table_occurrence = ADDRESS_PORTAL_TABLE_OCCURRENCE

    city = fmdata.String(
        field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::City",  # FM field name
        field_type=FMFieldType.Text,  # FM field type
    )
    zip = fmdata.String(
        field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::Zip",
        field_type=FMFieldType.Number,
    )
    reviewed_at = fmdata.DateTime(
        field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::ReviewedAt",
        field_type=FMFieldType.Text,  # stored as text in FileMaker
    )
    attachment = fmdata.Container(
        field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::Attachment",
    )

    population = fmdata.Integer(
        field_name=f"{ADDRESS_CITY_INFO_TABLE_OCCURRENCE}::Population",
        field_type=FMFieldType.Number,
    )


ADDRESS_PORTAL_NAME = "person_addresses"
ADDRESS_SORTED_BY_CITY_PORTAL_NAME = "person_addresses_sorted_by_city"


class Person(fmdata.Model):
    class Meta:
        # The `Client` instance to use and the **layout name** in FileMaker
        client = client
        layout = "person"  # this layout should be in "Form View" in FileMaker

    pk = fmdata.String(
        field_name="PrimaryKey",
        read_only=True,
        field_type=FMFieldType.Text
    )
    creation_timestamp = fmdata.DateTime(
        field_name="CreationTimestamp",
        read_only=True,
        field_type=FMFieldType.Timestamp, )

    name = fmdata.String(field_name="Name", field_type=FMFieldType.Text)
    last_name = fmdata.String(field_name="LastName", field_type=FMFieldType.Text)
    birth_date = fmdata.Date(field_name="BirthDate", field_type=FMFieldType.Date)
    join_time = fmdata.DateTime(field_name="JoinTime", field_type=FMFieldType.Timestamp)
    is_active = fmdata.Bool(field_name="IsActive", field_type=FMFieldType.Number)
    id_card_file = fmdata.Container(field_name="IDCardFile", field_type=FMFieldType.Container)
    phone_1 = fmdata.String(field_name="Phone(1)", field_type=FMFieldType.Text)
    phone_2 = fmdata.String(field_name="Phone(2)", field_type=FMFieldType.Text)

    addresses = fmdata.PortalField(model=AddressPortal, name=ADDRESS_PORTAL_NAME)
    addresses_sorted_by_city = fmdata.PortalField(model=AddressPortal, name=ADDRESS_SORTED_BY_CITY_PORTAL_NAME)
```

### Field types: Python vs FileMaker

Each field declaration is split conceptually in two halves:

**Left part (Python side)**

- The attribute name (for example `name`, `birth_date`, `is_active`), this is
  the name you will use **everywhere in Python**:
    - accessing attributes (`person.name`),
    - building queries (`find(name="Alice")`,`order_by("-birth_date")`),
    - serializing to Dict (`person.to_dict()`).
- The Python type you work with (`String`, `Integer`, `Float`, `Decimal`,
  `Bool`, `Date`, `DateTime`, `Time`, `Container`). This controls how values
  are validated, serialized to FileMaker and deserialized back.

> #### Some attribute names are forbidden:
>   - they cannot start with `_`,
>   - they cannot contain `__`,
>   - they cannot be one of the reserved names like (`record_id`, `mod_id`, `portal_name`, `table_occurrence`, `model`,
      `portal`, `layout`).

**Right part (FileMaker side)**

- `field_name`: the actual FileMaker field name. For model fields pointing to
  the layout's base table this is just the field name (for example `"Name"`).
  For fields belonging to another table or used in portals you should use the
  full `tableOccurrence::FieldName` form (for example
  `"person_addresses::City"`).
    - If the underlying FileMaker field is a **repeating** field and you want to bind a
      specific repetition, you must include the repetition index in the
      `field_name`. Use the `FieldName(N)` syntax where `N` starts from **1**.
      For example, to map the first and second repetitions of a `Phone` field:

      ```python
      phone_1 = fmdata.String(field_name="Phone(1)", field_type=FMFieldType.Text)
      phone_2 = fmdata.String(field_name="Phone(2)", field_type=FMFieldType.Text)
      ```
- `field_type`: the FileMaker field type (`FMFieldType.Text`,
  `FMFieldType.Number`, `FMFieldType.Date`, `FMFieldType.Timestamp`,
  `FMFieldType.Time`, `FMFieldType.Container`).
- `read_only`: whether the field is **read-only** from the ORM point of view.
  When `read_only=True`, `fmdata` will **never attempt to write** to that
  field in FileMaker.

  You should mark a field as `read_only=True` in all these cases:

    - the FileMaker field is a **calculated field** (Formula/Calculation);
    - the FileMaker field is **not editable** (for example "Prohibit modification of value during data entry" is checked
      in the field's properties);
    - you want to be extra safe and ensure that a field is **never modified accidentally** via the ORM.

  Example:

  ```python
  pk = fmdata.String(
      field_name="PrimaryKey",
      field_type=FMFieldType.Text,
      read_only=True,
  )
  ```

The mapping between Python field classes and FileMaker field types is validated
internally. Only the following combinations are allowed:

| Python field class (`fmdata`) | Allowed FileMaker field types (`FMFieldType`)              |
|-------------------------------|------------------------------------------------------------|
| `String`                      | `Text`, `Number`, `Date`, `Timestamp`, `Time`, `Container` |
| `Integer`                     | `Number`, `Text`                                           |
| `Float`                       | `Number`, `Text`                                           |
| `Decimal`                     | `Number`, `Text`                                           |
| `Bool`                        | `Number`, `Text`                                           |
| `Date`                        | `Date`, `Text`                                             |
| `DateTime`                    | `Timestamp`, `Text`                                        |
| `Time`                        | `Time`, `Text`                                             |
| `Container`                   | `Container`                                                |

### Field type limitations and gotchas

- **Float precision**
    - When you use `Float` on the Python side, you can **lose precision when
      reading from FileMaker**, both for `FMFieldType.Number` and
      `FMFieldType.Text`.
    - If you need full precision for numeric values (for example money or long
      decimals), prefer `Decimal` or `String` on the Python side instead of
      `Float`.
    - In practice, using `Float` is almost never a good idea for persisted data.

- **DateTime and timezone handling**
    - `DateTime -> FMFieldType.Timestamp`:
        - FileMaker `Timestamp` format does **not store timezone information** and
          does **not store sub‑second precision** (milliseconds / microseconds).
        - If your Python `datetime` has timezone or sub-second info, they will be **stripped** when
          written to FileMaker.
    - `DateTime -> FMFieldType.Text`:
        - When you map a Python `DateTime` to `FMFieldType.Text`, the full ISO
          representation is written (`YYYY-MM-DDTHH:MM:SS.ffffff[±TZ]`).
            - In this case **no timezone/sub-second information is lost**, you get back the same
              ISO string.
    - `String -> FMFieldType.Timestamp`:
        - The string must be a valid ISO representation of `datetime`.
        - If the ISO string carries timezone or sub‑second information, FileMaker `Timestamp`
          still cannot store it, so they will be **stripped**.

- **Container fields**
    - Use `Container` to work with FileMaker container fields.
    - To **update** a container value you must use the special API:

      ```python
      with open("/path/to/file.pdf", "rb") as file:
        person.update_container("id_card_file", file)
      person.refresh_from_db()  # If you want to read the updated container URLs
      ```

      This ensures that the container data is uploaded correctly and the record
      is refreshed with updated URLs.

- **Bool fields (true/false mapping)**
    - `Bool` can be mapped to either `FMFieldType.Number` or `FMFieldType.Text`.
    - `Bool` has sensible defaults for what is considered true/false and what is
      written to FileMaker, so you don't have to configure anything in the
      simplest cases.
    - When defining a `Bool` you can **optionally** customize how values are
      written to FileMaker and how incoming values are interpreted by using the
      `true_value`, `false_value`, `truthy` and `falsy` arguments:

      ```python
      is_active = fmdata.Bool(
          field_name="IsActive",
          field_type=FMFieldType.Number,
          true_value="1",   # value written when Python value is True
          false_value="0",  # value written when Python value is False
          truthy=["1", "true", 1],   # values considered True when reading from FM
          falsy=["0", "false", 0],  # values considered False when reading from FM
      )
      ```

    - Any value not contained in `truthy`/`falsy` will raise a `ValidationError`
      when deserializing from FileMaker.

## Working with records

### Model records

##### Find all records

```python
people = Person.objects.all()
```

#### Find with conditions

Find all person called `Alice`, born after 1990, ordered by `BirthDate` DESC, with a limit of 10 records

```python
people = Person.objects.find(name="Alice", birth_date__gt=date(1990, 1, 1)).order_by("-birth_date")[:10]
```

#### Iterate over the result set

```python
for person in people:
    print(f"Person: {person.name} - {person.last_name}")
```

#### Result set as a list

```python
list = list(people)
```

#### Count the number of results

```python
count = len(people)
```

#### Create a new record (and save it to the database)

```python
person = Person.objects.create(
    name="John",
    last_name="Doe",
    birth_date=date(1990, 1, 1)
)
```

or

```python
person = Person(
    name="John",
    last_name="Doe",
    birth_date=date(1990, 1, 1)
)

person.save()
```

#### Update an existing record

```python
person.name = "John Albert"
person.birth_date = date(1990, 1, 1)
person.save()
```

#### Delete a record

```python
person.delete()
```

#### Refresh a record from the database

```python
person.refresh_from_db()
```

#### Read a record given a record_id

```python
person = Person.objects.get(record_id="123")
```

### Portal records

#### Read portal records of a model record

```python
addresses = person.addresses.all()
```

#### Iterate over the result set

```python
for address in addresses:
    print(f"Address: {address.city} - {address.zip}")
```

#### Result set as a list

```python
list = list(people)
```

#### Count the number of results

```python
count = len(addresses)
```

#### Create a portal record (and save it to the database)

Pay attention: In FMS17 the Data API don't return any information about the portal created, so it's impossible to know
the record_id of the portal record.  
Because of this, the `.create()` will execute the portal creation but will return
`None`.

```python
address = person.addresses.create(city="New York", zip=10001)
```

#### Update a portal record

```python
address.zip = 10002
address.city = "New York City"
address.save()
```

#### Delete a portal record

```python
address.delete()
```

#### Create a portal record (without saving it to the database)

Avoid it in FMS17! The `.save()` will not update the record_id of the record, so the next `.save()` will create another
record!

```python
address = person.addresses.new(city="New York", zip=10001)
# Do something with the record
address.reviewed_at = datetime.now()
# Save the record later
address.save() 
```

### Saving records: full semantics of `record.save()`

Both `Model` records and `PortalModel` records provide the `save()` method, which accept **optional**
arguments to control **what** is written and **how** the Data API call behaves:

> #### `check_mod_id` (safe concurrent update)
>
>```python
>person.name = "Concurrent Safe Name"
>person.save(check_mod_id=True)
>```
>
>- Default: `False`.
>- When `True`, FileMaker will check that the current record `mod_id` in the database **matches** the `mod_id` on the
   > model before applying the update.
>- If someone else modified the record in the meantime (so the `mod_id` in FileMaker changed), the Data API call will
   > fail and `fmdata` will raise an exception.
>
>This lets you implement **optimistic locking**: you can be sure that you are not silently overwriting someone else’s
> changes.


> ##### `force_insert` (soft cloning)
>
>```python
># Assume `person` is already persisted and has a record_id
># Create a **new** record in FileMaker, with the same data as the existing one:
>soft_cloned = person.save(force_insert=True)
>```
>
>- If the record **already has** a `record_id` (so it is persisted in the DB), `force_insert=True` will create a **new
   > record** in FileMaker based on the data contained in the model: this is effectively a soft **clone**.
>- If there is **no** `record_id` yet, `force_insert=True` behaves like a normal create.


> ##### `force_update` (require an existing record)
>
>```python
>person = Person(name="Transient")
>
># This will raise, because person.record_id is None
>person.save(force_update=True)
>```
>
>- When `force_update=True`, `save()` will **raise an error** if `record_id` is not present yet.
>- This is useful when you want to be absolutely sure you are **not accidentally creating** new records.


> ##### `only_updated_fields` (send only changed fields)
>
>```python
>person.name = "Minimal Patch"
>person.save(only_updated_fields=True)   # default behaviour, only send the updated fields
>person.save(only_updated_fields=False)  # save all the fields including the unchanged ones
>```
>
>- Default is `True` for normal creates and updates: only the fields that actually changed on the model/portal are
   > sent to FileMaker.
>- When `False`, **all fields defined on the model** are included in the update (subject to `update_fields`, see
   > below).
>- For cloning (`force_insert=True` on an existing record), the default becomes `False`, because you usually want to
   > copy everything into the new record.

> ##### `update_fields` (restrict the fields being saved)
>
>```python
>person.name = "John Only-Name"
>person.last_name = "Doe Ignored"
>
># Only the "name" field will be persisted
>person.save(update_fields=["name"])
>```
>
>- `update_fields` is a list of field names that restricts **which fields are allowed to be written**.
>- It works **together** with `only_updated_fields`:
>  - First, we compute the set of fields that would normally be updated (either only the updated ones, or all of them
>   if `only_updated_fields=False`).
>  - Then, we intersect that set with `update_fields`.
>- The default is `None`, which means “no extra restriction” (all candidate fields are written).

## Bulk operations

### Bulk operations on portal records

- **Bulk create portal records**  
  You can create or update several portal records in **one single Data API call** by using `new()` and then calling
  `save()` on the parent model.
  ```python
  # Create portal rows in memory
  new_home = person.addresses.new(city="Home City", zip="12345")
  new_office = person.addresses.new(city="Office City", zip="67890")

  # Persist all in ONE FileMaker Data API call
  person.save(portals=[new_home, new_office])
  ```

- **Bulk update portal records**

  ```python
  addresses = person.addresses.all()
  for addr in addresses:
      if addr.city == "Old City":
          addr.city = "New City"
          addr.zip = "10001"

  # Persist all changes for the given portal rows in ONE call
  person.save(portals=addresses)
  ```

- **Bulk delete portal records**

  ```python
  addresses = person.addresses.all()
  to_delete = [addr for addr in addresses if addr.city == "To delete"]

  person.save(portals_to_delete=to_delete)
  ```

### Transactions and mixed operations

The FileMaker Data API allows you to perform at most **one edit operation on a parent record plus a combination of
creates / updates / deletes on its portal records in a single call**. `fmdata` exposes this pattern via
`model.save()`.

Example of **edit + update + create + delete** in a *single* call:

```python
person.surname = "Another Smith"  # edit parent model

addresses = person.addresses.all()
first_address = addresses[0]
first_address.city = "New York"  # update existing portal row

address_to_delete = addresses[-1]

new_address = person.addresses.new({  # new portal row 
    "city": "Las Vegas",
    "zip": "89109",
})

person.save(
    portals=[first_address, new_address],
    portals_to_delete=[address_to_delete],
)

```

This translates into a single Data API request that updates the parent record, updates one portal row, creates one
portal row, and deletes another.

### Full semantics of `model.save(portals=..., portals_to_delete=...)`

The `portals`‑related arguments on `model.save()` allow you to atomically
create, update and delete **portal rows** together with the parent record in
one Data API call.

```python
person.save(
    portals=...,  # create / update these portal rows
    portals_to_delete=...,  # delete these portal rows
)
```

The full semantics are:

- `portals`: iterable of portal records to **create or update**.
    - Each item can be either:
        - a `PortalModel` instance, or
        - a `SavePortalsConfig` instance (from `fmdata.orm`) wrapping a
          `PortalModel` and customizing how it is saved.
    - When you pass a **plain** `PortalModel` the behavior is the same as
      calling `portal.save()` with same defaults.
    - When you need to tweak `check_mod_id`, `update_fields` or
      `only_updated_fields` for a specific portal row, wrap it into
      `SavePortalsConfig` and pass the wrapper instead of the bare
      `PortalModel`.

- `portals_to_delete`: iterable of portal records to **delete**.
    - Each item must be a `PortalModel` that already has a `record_id`

#### Example: Use `SavePortalsConfig` to control per‑row options

```python
from fmdata.orm import SavePortalsConfig

addresses = person.addresses.all()
first = addresses[0]
second = addresses[1]

first.city = "Optimistic City"  # we want optimistic locking here
second.city = "Partial Update"  # we only want to update the city field
second.zip = "99999"  # this change will be ignored

configs = [
    # Update `first` using its mod_id for optimistic locking
    SavePortalsConfig(
        portal=first,
        check_mod_id=True,
    ),

    # Update only the `city` field for `second`
    SavePortalsConfig(
        portal=second,
        update_fields=["city"],
    ),
]

person.save(portals=configs)
```

### Bulk operations on model records

Bulk operations on **top-level model records** are implemented in a Django-like way, but `fmdata` still
performs **one API call per record**:

- **Bulk delete**

  ```python
  (
      Person
      .objects()
      .find(is_active=False)
      .delete()   # one API call per record
  )
  ```

- **Bulk update**

  ```python
  (
      Person
      .objects()
      .find(city="Old City")
      .update({"city": "New City"})  # one API call per record
  )
  ```

## Advanced querying

The ORM offers a rich querying API very similar to Django's: criteria, sorting, prefetching, offset/limit and
chunked iteration.

### Criteria

You can filter records using simple keyword arguments, field lookups and also raw criteria:

```python
# Basic equality (exact match)
people = Person.objects.find(name="Alice")

# String operators (__startswith, __endswith, __contains)
people = Person.objects.find(name__contains="Alice")
people = Person.objects.find(name__startswith="Alice")
people = Person.objects.find(name__endswith="Smith")

# Comparison operators (__gt, __gte, __lt, __lte)
people = Person.objects.find(birth_date__gt=date(1990, 1, 1))

# Range operator (__range)
people = Person.objects.find(age__range=(18, 30))
people = Person.objects.find(birth_date__range=(date(1990, 1, 1), date(2000, 12, 31)))

# Multiple criteria are AND‑ed by default
people = Person.objects.find(name="Alice", is_active=True)

# Raw criteria (using FileMaker's own query syntax)
people = Person.objects.find(name__raw="Alice*", last_name__raw="*Sm*")
```

Behind the scenes each call to `.find()` generates one or more **Find
requests** to FileMaker. You can also explicitly build **Find/Omit** series:

- Every *Find* block **adds** records to the result set.
- Every *Omit* block **removes** records from the current result set.
- The order of the Find/Omit operations matters: they are executed **in
  sequence**, one after the other.

`fmdata` exposes helpers so you can express this from Python while still using
your model field names and Python types. For example:

```python
# Find people named Alice OR Bob, then omit inactive ones
people = (
    Person
    .objects
    .find(name__in=["Alice", "Bob"])  # first Find
    .omit(is_active=False)  # then Omit
)

# Find people born after a certain date, then omit those living in a city
people = (
    Person
    .objects
    .find(birth_date__gte=date(1990, 1, 1))
    .omit(city="Old City")
)
```

There are pre-built criteria converters (see `orm._process_omit_kwargs`)
that interpret your keyword arguments based on the **Python field type**:

- For `Date` fields you pass `datetime.date` instances.
- For `DateTime` fields you pass `datetime.datetime` instances.
- For numeric fields (`Integer`, `Decimal`, etc.) you pass Python numbers.
- For `String` fields you pass Python strings.

This keeps your code type-safe and readable while fmdata converts everything
into the FileMaker query format.

### Sorting

Sorting can be chained and supports ascending / descending order for multiple fields:

```python
people = (
    Person
    .objects
    .find(is_active=True)
    .order_by("birth_date", "-last_name")  # ASC by birth_date, DESC by last_name
)
```

The minus sign (`-`) means **descending** order.

### Prefetching

`prefetch()` allows you to load related portal records together with the parent records in a single round trip to the
server:

```python
people = (
    Person
    .objects
    .find(is_active=True)
    .prefetch("addresses", "addresses_sorted_by_city")
)

for person in people:
    # No extra API call for portal access, they are already loaded
    for address in person.addresses.all():
        print(address.city)
```

When a portal is prefetched, accessing it later on the same model instance will **always return the prefetched
records**, even if other queries would now return a different set.

If you want to explicitly bypass prefetched data and re-load from the server, use `ignore_prefetched()` on your query:

```python
people = (
    Person
    .objects
    .find(is_active=True)
    .prefetch("addresses")
)

# Later, run a query that must ignore prefetched portals
fresh_people = (
    Person
    .objects
    .ignore_prefetched()  # forces a new call instead of reusing prefetched portals
    .find(is_active=True)
)
```

You can also prefetch only a **slice of a portal** by using `offset` and
`limit` arguments. For example, to prefetch at most 100 `addresses` portal
rows starting from the 3rd record:

```python
people = (
    Person
    .objects
    .find(is_active=True)
    .prefetch("addresses", offset=2, limit=100)
)
```

### Offset / Limit (slicing)

Result sets are sliceable using the familiar Python slice syntax. Internally this is translated into `offset` and
`limit` parameters for the FileMaker Data API:

```python
# Retrieve records 10..19
people = Person.objects.find(is_active=True)[10:20]

# First 10 records
first_page = Person.objects.find(is_active=True)[:10]

# Single record at position N
person = Person.objects.find(is_active=True)[5]

# You can slice portal relations as well
person = Person.objects.get(record_id="123")
first_two_addresses = person.addresses.all()[0:2]
next_page_of_addresses = person.addresses.all()[2:4]
```

### Chunking

Both model records and portal records can be iterated in **chunks** to avoid
timeouts and control the number of records fetched per request. Chunking lets
you process large datasets in multiple smaller API calls while keeping memory
usage under control:

```python
for active_people in Person.objects.find(is_active=True).chunked(1000):
    for person in active_people:
        process(person)
```

The same idea applies to portals:

```python
for addresses in person.addresses.all().chunked(1000):
    for address in addresses:
        process(address)
```

> **Warning about coherence**
>
> Chunked iteration is very powerful, but since each chunk is a separate call to the Data API, **the underlying
> dataset can change between chunks** (inserts, updates, deletions). This can introduce:
>
> - **Holes**: some records might be skipped if previous records are deleted between calls.
> - **Order shifts**: if records are inserted or deleted before the current offset, later chunks may return records you
    > would not expect at that position, or in a slightly different order.
>
> For fully consistent snapshots, avoid concurrent modifications while iterating, or load all data at once if the
> dataset is small enough.

## Model utilities
### Converting a portal row to a layout model (`as_layout_model`)

Sometimes you have a portal record and you want to work with it as if it were
coming from a dedicated layout (for example to reuse an existing model class,
map fields with different Python names, or upload a container field that
belongs to that layout). `portal_record.as_layout_model(model_class)` lets you do exactly
this.

Consider this portal model and a corresponding layout model:

```python
class AddressPortal(PortalModel):
    class Meta:
        table_occurrence = ADDRESS_PORTAL_TABLE_OCCURRENCE

    city = fmdata.String(field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::City", field_type=FMFieldType.Text, )
    picture = fmdata.Container(field_name=f"{ADDRESS_PORTAL_TABLE_OCCURRENCE}::Picture", )


class AddressLayoutModel(Model):
    class Meta:
        layout = "address_layout"  # a layout based on the same table

    # Note how the Python name can differ from the portal field name
    the_city = fmdata.String(field_name="City", field_type=FMFieldType.Text)
    picture = fmdata.Container(field_name="Picture", field_type=FMFieldType.Container)


# Given a portal record
address_portal_record = person.addresses.all()[0]
# Convert the portal row to the layout model
address_as_layout_record = address_portal_record.as_layout_model(model_class=AddressLayoutModel)
```

#### Updating a portal container field

The Data API does not support updating a portal container field directly. So we use `as_layout_model` to convert the
given portal record to a layout model record and then use `update_container()` on that:

```python
# Given a portal record
address_portal_record = person.addresses.all()[0]
# Convert the portal row to the layout model
address_as_layout_record = address_portal_record.as_layout_model(model_class=AddressLayoutModel)

# Upload a file to the portal container field via the layout model
with open("/path/to/file.pdf", "rb") as file:
    address_as_layout_record.update_container("picture", file)
```


## Low-Level API Access

For direct FileMaker Data API access:

```python
# Direct API calls
result = fm_client.create_record(
    layout="people",
    field_data={"FullName": "Jane Doe", "EnrollmentDate": "01/15/2024"}
).raise_exception_if_has_error()  # Raise FileMakerErrorException if response contains an error message

# Get record by ID
record = fm_client.get_record(layout="people", record_id="123").raise_exception_if_has_error()

# Perform find
results = fm_client.find(
    layout="people",
    query=[{"FullName": "John*"}],
    sort=[{"fieldName": "FullName", "sortOrder": "ascend"}]
).raise_exception_if_has_error()

# Execute scripts
script_result = fm_client.perform_script(
    layout="people",
    name="MyScript",
    param="parameter_value"
).raise_exception_if_has_error()
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.

## Author

Lorenzo De Siena (dev.lorenzo.desiena@gmail.com)

## Acknowledgements

We would like to thank:

- **[EMBO (European Molecular Biology Organization)](https://www.embo.org/)**
- **[Django](https://github.com/django/django)** for the ORM inspiration
- **[python-fmrest](https://github.com/davidhamann/python-fmrest)** for inspiration

## Links

- GitHub: https://github.com/Fenix22/python-fmdata
- PyPI: https://pypi.org/project/fmdata/
