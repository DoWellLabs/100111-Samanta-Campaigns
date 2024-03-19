from rest_framework.exceptions import ValidationError
from django.utils import timezone
import datetime
from typing import IO, Iterable
import uuid

from .utils import is_email, is_phonenumber, is_valid_url
from .dowell.user import DowellUser



def validate_not_blank(value):
    """
    Validates that value is not blank, empty or None

    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if not value:
        raise ValidationError('This field cannot be blank.')
    return None
    

def validate_not_in_past(value: datetime.datetime | datetime.date | datetime.time):
    """
    Validates that value is not in the past

    :param value: date/datetime/time value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if isinstance(value, datetime.date) and value < timezone.now().date():
        raise ValidationError('This date field cannot be in the past.')
    elif isinstance(value, datetime.datetime) and value < timezone.now():
        raise ValidationError('This datetime field cannot be in the past.')
    elif isinstance(value, datetime.time) and value < timezone.now().time():
        raise ValidationError('This time field cannot be in the past.')
    return None


 
class MaxDurationValidator:
    """
    Validates that duration does not exceed maximum duration specified.
    """
    def __init__(self, **durations):
        """
        Instantiate object

        :param durations: Maximum duration in keyword arguments. E.g. hours=1, minutes=30
        """
        self.max_duration = timezone.timedelta(**durations)


    def __call__(self, duration: timezone.timedelta):
        """
        Validate duration

        :param duration: Duration to validate
        :return: None
        :raises: `django.core.exceptions.ValidationError`
        """
        if duration > self.max_duration:
            raise ValidationError(f"Duration cannot be greater than {self.max_duration}")
        return None



class FileSizeValidator:
    """
    Validates that file size does not exceed maximum size specified in Megabytes
    """
    def __init__(self, max_size: int=1):
        """
        Instantiate object

        :param max_size: Maximum size in Megabytes
        """
        self.max_size = max_size


    def __call__(self, file: IO):
        """
        Validate file size

        :param image: file
        :return: None
        :raises: `django.core.exceptions.ValidationError`
        """
        if file.size > self.max_size * 1024 * 1024:
            raise ValidationError(f"file size must be not be greater than {self.max_size}MB")
        return None




class MinMaxLengthValidator:
    """
    Validates that value is not less than minimum length and not greater than maximum length
    """
    def __init__(self, min_length: int=None, max_length: int=None):
        """
        Instantiate object

        :param min_length: Minimum length
        :param max_length: Maximum length
        """
        if min_length is None and max_length is None:
            raise ValueError("At least one of min_length or max_length must be specified")
        if min_length and not isinstance(min_length, int):
            raise ValueError("min_length must be an integer")
        if max_length and not isinstance(max_length, int):
            raise ValueError("max_length must be an integer")
        self.min_length = min_length
        self.max_length = max_length
        return None


    def __call__(self, value: str):
        """
        Validate value

        :param value: value to validate
        :return: None
        :raises: `django.core.exceptions.ValidationError`
        """
        if self.min_length and len(value) < self.min_length:
            raise ValidationError(f"Length must be at least {self.min_length}")
        if self.max_length and len(value) > self.max_length:
            raise ValidationError(f"Length must not be greater than {self.max_length}")
        return None


def validate_email(value: str):
    """
    Validate that value is a valid email
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if not is_email(value):
        raise ValidationError("This field must be a valid email")
    return None


def validate_phonenumber(value: str):
    """
    Validate that value is a valid phone number
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if not is_phonenumber(value):
        raise ValidationError("This field must be a valid phone number")
    return None


def validate_email_or_phone_number(value: str):
    """
    Validate that value is either an email or phone number
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if not is_email(value) and not is_phonenumber(value):
        raise ValidationError("This field must be either an email or phone number")
    return None


def contains_only_emails(iterable: Iterable):
    """
    Validate that iterable contains only emails
    :param iterable: iterable to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    for val in iterable:
        try:
            validate_email(val)
        except ValidationError:
            raise ValidationError("This field must contain only emails.")
    return None


def contains_only_phone_numbers(iterable: Iterable):
    """
    Validate that iterable contains only phone numbers
    :param iterable: iterable to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    for val in iterable:
        try:
            validate_phonenumber(val)
        except ValidationError:
            raise ValidationError("This field must contain only phone numbers.")
    return None


def contains_only_emails_and_numbers(iterable: Iterable):
    """
    Validate that iterable contains only emails or phone numbers or both
    :param iterable: iterable to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    for val in iterable:
        try:
            validate_email_or_phone_number(val)
        except ValidationError:
            raise ValidationError("This field must contain only emails or phone numbers or both.")
    return None


def is_valid_workspace_id(value: str):
    """
    Check if the workspace id is valid
    
    :param value: workspace id
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    try:
        DowellUser(value)
    except Exception as exc:
        raise ValidationError(exc)
    return None


def validate_uuid(value: str):
    """
    Validate that value is a valid UUID
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    try:
        uuid.UUID(value)
    except ValueError:
        raise ValidationError("This field must be a valid UUID")
    return None


def is_api_key(value: str):
    """
    Validate that value is a valid API key
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    try:
        validate_uuid(value)
    except ValidationError:
        raise ValidationError("This field must be a valid API key")
    return None


def validate_url(value: str):
    """
    Validate that value is a valid URL
    :param value: value to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    if not is_valid_url(value):
        raise ValidationError("This field must be a valid URL")
    return None


def contains_only_urls(iterable: Iterable):
    """
    Validate that iterable contains only URLs
    :param iterable: iterable to validate
    :return: None
    :raises: `django.core.exceptions.ValidationError`
    """
    for val in iterable:
        try:
            validate_url(val)
        except ValidationError:
            raise ValidationError("This field must contain only URLs.")
    return None
