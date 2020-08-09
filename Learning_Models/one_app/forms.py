from django import forms
from one_app.models import User


class NewUserForm(forms.ModelForm):
    class Meta():
        model = User
        fields = '__all__'
