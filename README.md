# python-fmdata

A Python wrapper for the FileMaker Data API with Django-style ORM functionality.

## Overview

`python-fmdata` is a comprehensive Python library that provides both low-level access to the FileMaker Data API and a high-level ORM (Object-Relational Mapping) interface similar to Django's ORM. It supports multiple authentication methods and makes it easy to work with FileMaker databases from Python applications.

## Features

- **Django-style ORM**: Query FileMaker databases using familiar ORM patterns
- **Multiple Authentication Methods**: Username/password, OAuth, and Claris Cloud support
- **Portal Relationships**: Full support for FileMaker portal (related) records
- **Efficient Querying**: Chunked results, prefetching, and pagination support
- **CRUD Operations**: Create, read, update, and delete records with ease
- **Low-level API Access**: Direct access to FileMaker Data API when needed
- **Type Safety**: Built with type hints and marshmallow for data validation

## Installation

```bash
pip install fmdata
```

For Claris Cloud support:
```bash
pip install fmdata[cloud]
```

## Requirements

- Python 3.8+
- FileMaker Server with Data API enabled
- Valid FileMaker database with appropriate privileges

## Quick Start

### 1. Setup Connection and Authentication

```python
import fmdata
from fmdata.session_providers import UsernamePasswordSessionProvider

# Create session provider
session_provider = UsernamePasswordSessionProvider(
    username="your_username",
    password="your_password"
)

# Create FileMaker client
fm_client = fmdata.FMClient(
    url="https://your-filemaker-server.com",
    database="your_database",
    login_provider=session_provider
)
```

### 2. Define Models

```python
from marshmallow import fields
from fmdata.orm import Model, PortalField, PortalModel

class ClassPortal(PortalModel):
    name = fields.Str(required=False, data_key="class_portal_name::Name")
    description = fields.Str(required=False, data_key="class_portal_name::Description")

class Student(Model):
    class Meta:
        client = fm_client
        layout = 'student_layout'

    pk = fields.Str(data_key="PrimaryKey")
    full_name = fields.Str(data_key="FullName")
    enrollment_date = fields.Date(data_key="EnrollmentDate")
    graduation_year = fields.Integer(data_key="GraduationYear", allow_none=True)

    # Portal relationship
    classes = PortalField(model=ClassPortal, name="class_portal_name")
```

### 3. Query Records

```python
# Find all students, ordered by primary key
students = Student.objects.order_by("pk").find(full_name__raw="*")

# Find students with exact match (equivalent to __exact)
student_john = Student.objects.find(full_name="John Doe")  # Searches for exact match
students_of_2024_but_not_john = Student.objects.find(graduation_year=2024).omit(full_name="John Doe") # Searches for students graduating in 2024 but not named John Doe

# Query with chunking and portal prefetching
result_set = (Student.objects
              .order_by("pk")
              .find(full_name__raw="*") # __raw means filemaker raw query, so it will search for all students with a non-empty full_name
              .chunk_size(1000) # Call the API in chunks of 1000 records (in this example, it will return all students)
              .prefetch_portal("classes", limit=100)
              )[:1000]  # Limit to first 1000 records

for student in result_set:
    print(f"Student: {student.pk} - {student.full_name}")

    # Access prefetched portal records
    for class_record in student.classes.only_prefetched():
        print(f"  Class: {class_record.name} - {class_record.description}")
```

### 4. Create, Update, and Delete Records

```python
# Create a new student
student = Student.objects.create(
    full_name="John Doe",
    enrollment_date=date(2024, 1, 15),
    graduation_year=2028
)

# Update a record
student.full_name = "John Smith"
student.save()

# Create portal records
student.classes.create(name="Mathematics", description="Advanced Math Course")

# Delete records
student.delete()

# Bulk operations
Student.objects.find(graduation_year=2024).delete()
```

## Authentication Methods

### Username/Password Authentication

```python
from fmdata.session_providers import UsernamePasswordSessionProvider

session_provider = UsernamePasswordSessionProvider(
    username="your_username",
    password="your_password"
)
```

### OAuth Authentication

```python
from fmdata.session_providers import OAuthSessionProvider

session_provider = OAuthSessionProvider(
    oauth_request_id="your_oauth_request_id",
    oauth_identifier="your_oauth_identifier"
)
```

### Claris Cloud Authentication

```python
from fmdata.session_providers import ClarisCloudSessionProvider

session_provider = ClarisCloudSessionProvider(
    claris_id_name="your_claris_id",
    claris_id_password="your_password"
)
```

## Advanced Querying

### Field Criteria

```python
from fmdata.orm import Criteria

# Various query criteria
students = Student.objects.find(
    full_name__contains="John",
    graduation_year__gte=2024,
    enrollment_date__range=(date(2020, 1, 1), date(2024, 12, 31))
)

# Raw FileMaker queries
students = Student.objects.find(full_name__raw="John*")

# Empty and non-empty fields
students = Student.objects.find(graduation_year__not_empty=True)
```

### Ordering and Pagination

```python
# Order by multiple fields
students = Student.objects.order_by("graduation_year", "-full_name").find()

# Pagination with slicing
first_10 = Student.objects.find()[0:10]
next_10 = Student.objects.find()[10:20]

# Chunked processing for large datasets
for student in Student.objects.find().chunk_size(1000):
    process_student(student)
```

### Portal Operations

```python
# Prefetch portal data
students = (Student.objects
            .prefetch_portal("classes", limit=50)
            .find())

# Work with portal records
for student in students:
    # Access portal records. If you are using a [:limit] and they are all prefetched, they will be accessed without additional API calls.
    classes = student.classes.all()[:30]  # Get the first 30 classes (in this example, all classes are prefetched because of the prefetch_portal with limit=50)

    # Access prefetched portals. This will return the full list of prefetched portal records without additional API calls.
    classes = student.classes.only_prefetched() 

    # Or force to fetch fresh portal data
    classes = student.classes.avoid_prefetch_cache() 

    # Create new portal records
    student.classes.create(name="New Class", description="Description")

    # Update portal records
    for class_record in classes:
        class_record.description += " (Updated)"
        class_record.save()

    # Delete portal records
    student.classes.filter(name="Old Class").delete()
```
## Error Handling

```python
from fmdata import FMErrorEnum
from fmdata.results import FileMakerErrorException

try:
    student = Student.objects.get(pk="nonexistent")
except FileMakerErrorException as e:
    if e.error_code in (FMErrorEnum.INSUFFICIENT_PRIVILEGES, FMErrorEnum.FIELD_ACCESS_DENIED):
        print("Insufficient privileges, please check your user permissions.")
    else:
        print(f"FileMaker error: {e.error_code} - {e.message}")
```

## Low-Level API Access

For direct FileMaker Data API access:

```python
# Direct API calls
result = fm_client.create_record(
    layout="Students",
    field_data={"FullName": "Jane Doe", "EnrollmentDate": "01/15/2024"}
).raise_exception_if_has_error() # Raise FileMakerErrorException if response contains an error message

# Get record by ID
record = fm_client.get_record(layout="Students", record_id="123").raise_exception_if_has_error()

# Perform find
results = fm_client.find(
    layout="Students",
    query=[{"FullName": "John*"}],
    sort=[{"fieldName": "FullName", "sortOrder": "ascend"}]
).raise_exception_if_has_error()

# Execute scripts
script_result = fm_client.perform_script(
    layout="Students",
    name="MyScript",
    param="parameter_value"
).raise_exception_if_has_error()
```


## Configuration Options

```python
fm_client = fmdata.FMClient(
    url="https://your-server.com",
    database="your_database",
    login_provider=session_provider,
    api_version="v1",  # API version
    connection_timeout=10,  # Connection timeout in seconds
    read_timeout=30,  # Read timeout in seconds
    verify_ssl=True,  # SSL certificate verification
    auto_manage_session=True  # Automatic session management
)
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
- **[python-fmrest](https://github.com/davidhamann/python-fmrest)** for inspiration

## Links

- GitHub: https://github.com/Fenix22/python-fmdata
- PyPI: https://pypi.org/project/fmdata/
