import json
from abc import ABCMeta, abstractmethod

from airgo.operators.python_operator import PythonOperator
from airgo.operators.base_operator import BaseOperator
from airgo.utils.decorators import artifact_property
from airgo.exceptions import AirgoRuntimeException
from datetime import datetime
from typing import Dict, Set


class ShortCircuitOperator(BaseOperator, metaclass=ABCMeta):
    """
    Expects a short_circuit_when property to be implemented and return a bool
    """

    @artifact_property(serializer=json.dumps)
    def short_circuit(self):
        """
        This property name is reserved for controlling how subsequent tasks are launched
        """
        val = self.short_circuit_when
        if not (isinstance(val, bool)):
            raise AirgoRuntimeException(
                "Return value of your short_circuit_when property must be a boolean, not {type(val)}"
            )
        return val

    def execute_syncronously(
        self, execution_set: Dict[str, Set[str]], context: Dict[str, datetime]
    ) -> None:
        super().execute_syncronously(execution_set, context)
        if self.short_circuit:
            execution_set["short_circuited"].add(self.task_id)

    @property
    @abstractmethod
    def short_circuit_when(self):
        raise (
            NotImplementedError(
                "Subclasses of ShortCircuitOperator must implement a `short_circuit_when` property"
            )
        )


class ShortCircuitPythonOperator(ShortCircuitOperator, PythonOperator):
    """
    Executes a Python callable to obtain a short-circuit bool
    """

    @property
    def short_circuit_when(self) -> bool:
        return self.return_value
