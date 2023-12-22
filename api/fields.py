from rest_framework import serializers


class ListField(serializers.ListField):
    """
    Serializer List field that saves list as string and returns list as representation
    
    To change the delimiter, pass delimiter=<delimiter> to the constructor.
    """
    delimiter = ','

    def __init__(self, *args, **kwargs):
        self.delimiter = kwargs.pop('delimiter', self.delimiter)
        super().__init__(*args, **kwargs)

    def to_internal_value(self, data):
        if data is None:
            return data
        return self.delimiter.join(str(item) for item in set(data))

    def to_representation(self, data):
        if isinstance(data, str):
            data = [ d.strip() for d in data.split(self.delimiter) ]
        return super().to_representation(data)



class CaseInsensitiveChoiceField(serializers.ChoiceField):
    def to_internal_value(self, data):
        # Normalize the input data to lowercase for case-insensitive matching
        data = data.lower()
        choices = [ choice[0] for choice in self.choices if isinstance(choice, (list, tuple)) ]
        choices.extend([ choice for choice in self.choices if isinstance(choice, str) ])
        choices_dict = {choice.lower(): choice for choice in choices}
        if data in choices_dict:
            return choices_dict[data]
        self.fail('invalid_choice', input=data)

    def to_representation(self, value):
        # Convert the stored value back to the original case
        choices = [ choice[0] for choice in self.choices if isinstance(choice, (list, tuple)) ]
        choices.extend([ choice for choice in self.choices if isinstance(choice, str) ])
        for choice in choices:
            if value.lower() == choice.lower():
                return choice
        self.fail('invalid_choice', value=value)
