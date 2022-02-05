from airgo.operators.base_operator import BaseOperator
from airgo.utils.decorators import apply_defaults
from airgo.exceptions import AirgoException
from typing import Optional, Iterable, Dict, Callable
from datetime import datetime


class PythonOperator(BaseOperator):
    """
    Executes a Python callable
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`
    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list (templated)
    """

    @apply_defaults
    def __init__(
        self,
        python_callable: Callable,
        op_args: Optional[Iterable] = None,
        op_kwargs: Optional[Dict] = None,
        provide_context: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if not callable(python_callable):
            raise AirgoException("`python_callable` param must be callable")

        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

    def execute(self, context: Dict[str, datetime]) -> None:
        if self.provide_context:
            context.update(self.op_kwargs)
            self.op_kwargs = context
        self.logger.info(
            f"Executing Callable with args: '{self.op_args}', kwargs: '{self.op_kwargs}'"
        )
        self.return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        self.logger.info(f"Done. Returned value was: {self.return_value}")
