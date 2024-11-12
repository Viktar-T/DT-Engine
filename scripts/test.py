class SimpleClass:
    def __init__(self, name):
        self.name = name

    def greet(self):
        return f"Hello, {self.name}!"

# test.py

class FirstClass:
    def __init__(self, value):
        self.value = value

    def display_value(self):
        print(f"Value: {self.value}")

class SecondClass:
    def __init__(self, name):
        self.name = name

    def greet(self):
        print(f"Hello, {self.name}!")