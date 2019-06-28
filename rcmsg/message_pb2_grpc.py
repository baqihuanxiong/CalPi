# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import message_pb2 as message__pb2


class CalculatorStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.MonteCarlo = channel.unary_unary(
        '/rcmsg.Calculator/MonteCarlo',
        request_serializer=message__pb2.CalRequest.SerializeToString,
        response_deserializer=message__pb2.CalReply.FromString,
        )


class CalculatorServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def MonteCarlo(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_CalculatorServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'MonteCarlo': grpc.unary_unary_rpc_method_handler(
          servicer.MonteCarlo,
          request_deserializer=message__pb2.CalRequest.FromString,
          response_serializer=message__pb2.CalReply.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'rcmsg.Calculator', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
