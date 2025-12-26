import json

class JsonToObjectDeserializationError(Exception):
    pass

class UnSupportedJsonTypes(Exception):
    pass

class BaseJsonSerializerClass(type):
    def __new__(cls, name, bases, dct):
        allowed_attributes=dict()
        supported_types=[int, str, list, dict]
        ustypes=dict()
        for k, v in dct.items():
            if not k.startswith("__"):
                if v in supported_types:
                    allowed_attributes[k]=v
                else:
                    ustypes[k]=v
        if ustypes:
            raise UnSupportedJsonTypes(ustypes)
        
        dct["allowed_attributes"]=allowed_attributes

        def init_fun(self, *args, **kwargs):
            if kwargs == {}:
                raise JsonToObjectDeserializationError("Invalid Arguments!")
            for k, v in kwargs.items():
                if allowed_attributes.get(k):
                    setattr(self, k, v)
                else:
                    raise JsonToObjectDeserializationError("Invalid attributes found!")
        
        def is_valid(self):
            for k, v in self.__dict__.items():
                if not allowed_attributes.get(k):
                    return False
                elif not isinstance(v, allowed_attributes[k]):
                    return False
            return True

        dct["__init__"]=init_fun
        dct["is_valid"]=is_valid

        return super().__new__(cls, name, bases, dct)

class JsonSerializerClass(metaclass=BaseJsonSerializerClass):
    pass

class JsonSerializer:
    def __init__(self, serializer_class) -> None:
        self.serializer_class=serializer_class
        self.format=format

    def load(self, file_path):
        with open(file_path) as f:
            kwargs=json.load(f)
            serializer_obj=self.serializer_class(**kwargs)
            return serializer_obj
        
    def dump(self, serializer_obj, file_path):
        if isinstance(serializer_obj, self.serializer_class) and serializer_obj.is_valid():
            d=serializer_obj.__dict__
            with open(file_path, "w") as f:
                return json.dump(d, f)
        else:
            raise JsonToObjectDeserializationError("Invalid Serializer object attributes!")

if __name__ == "__main__":
    class Test(JsonSerializerClass):
        a=int
        b=str
        c=str
        def __str__(self) -> str:
            return f"a={self.a}, b={type(self.b)}"
    js=JsonSerializer(Test)
    js.dump(Test(a=1, b="str", c="str"), "./test_obj.json")
    test_obj=js.load("./test_obj.json")
    print(test_obj)