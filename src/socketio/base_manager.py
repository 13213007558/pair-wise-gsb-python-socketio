import itertools
import logging
import json

from bidict import bidict, ValueDuplicationError

default_logger = logging.getLogger('socketio')


class Ack:
    """A registered acknowledgement callback.

    The wrapper keeps the application callback together with an optional
    ``on_disconnect`` hook. The hook is invoked by the manager when the
    client leaves the namespace while the acknowledgement is still pending,
    which allows synchronous request/response calls (see
    :meth:`Server.call`) to be aborted instead of waiting out their full
    timeout.
    """
    def __init__(self, callback, on_disconnect=None):
        self.callback = callback
        self.on_disconnect = on_disconnect

    def __call__(self, *args):
        return self.callback(*args)

    def disconnected(self):
        if self.on_disconnect is not None:
            self.on_disconnect()


class BaseManager:
    def __init__(self):
        self.logger = None
        self.server = None
        self.rooms = {}  # self.rooms[namespace][room][sio_sid] = eio_sid
        self.eio_to_sid = {}
        self.callbacks = {}
        self.pending_connect = {}
        self.pending_disconnect = {}
        self.json = json

    def set_server(self, server):
        self.server = server
        self.json = self.server.packet_class.json  # use the global JSON module

    def initialize(self):
        """Invoked before the first request is received. Subclasses can add
        their initialization code here.
        """
        pass

    def get_namespaces(self):
        """Return an iterable with the active namespace names."""
        return self.rooms.keys()

    def get_participants(self, namespace, room):
        """Return an iterable with the active participants in a room.

        Note that in a multi-server scenario this method only returns the
        participants connect to the server in which the method is called. There
        is currently no functionality to assemble a complete list of users
        across multiple servers.
        """
        ns = self.rooms.get(namespace, {})
        if hasattr(room, '__len__') and not isinstance(room, str):
            participants = ns[room[0]]._fwdm.copy() if room[0] in ns else {}
            for r in room[1:]:
                participants.update(ns[r]._fwdm if r in ns else {})
        else:
            participants = ns[room]._fwdm.copy() if room in ns else {}
        yield from participants.items()

    def pre_connect(self, eio_sid, namespace):
        """Register a client connection to a namespace.

        The client is initially marked as pre-connected and later updated to
        fully connected once it passes authentication in the connect handler.
        """
        sid = self.server.eio.generate_id()
        try:
            self.basic_enter_room(sid, namespace, None, eio_sid=eio_sid)
        except ValueDuplicationError:
            # already connected
            return None
        self.basic_enter_room(sid, namespace, sid, eio_sid=eio_sid)
        if namespace not in self.pending_connect:  # pragma: no branch
            self.pending_connect[namespace] = []
        self.pending_connect[namespace].append(sid)
        return sid

    def connect(self, sid, namespace):
        """Register a connected client as fully connected.

        This marks the client as having passed authentication.
        """
        if not self.is_pre_connected(sid, namespace):
            # the client has not been pre-connected, this is a complete
            # connection attempt, and sid is actually eio_sid
            sid = self.pre_connect(sid, namespace)
        if namespace in self.pending_connect and \
                sid in self.pending_connect[namespace]:  # pragma: no branch
            self.pending_connect[namespace].remove(sid)
            if len(self.pending_connect[namespace]) == 0:  # pragma: no branch
                del self.pending_connect[namespace]
        return sid

    def is_pre_connected(self, sid, namespace):
        if namespace not in self.pending_connect or \
                sid not in self.pending_connect[namespace]:
            return False
        try:
            return self.rooms[namespace][None][sid] is not None
        except KeyError:  # pragma: no cover
            pass
        return False  # pragma: no cover

    def is_connected(self, sid, namespace):
        if (
            namespace in self.pending_connect
            and sid in self.pending_connect[namespace]
        ) or (
            namespace in self.pending_disconnect
            and sid in self.pending_disconnect[namespace]
        ):
            # the client is in the process of connecting or disconnecting
            return False
        try:
            return self.rooms[namespace][None][sid] is not None
        except KeyError:
            pass
        return False

    def sid_from_eio_sid(self, eio_sid, namespace):
        try:
            return self.rooms[namespace][None]._invm[eio_sid]
        except KeyError:
            pass

    def eio_sid_from_sid(self, sid, namespace):
        if namespace in self.rooms:
            return self.rooms[namespace][None].get(sid)

    def pre_disconnect(self, sid, namespace):
        """Put the client in the to-be-disconnected list.

        This allows the client data structures to be present while the
        disconnect handler is invoked, but still recognize the fact that the
        client is soon going away.
        """
        if namespace not in self.pending_disconnect:
            self.pending_disconnect[namespace] = []
        self.pending_disconnect[namespace].append(sid)
        return self.rooms[namespace][None].get(sid)

    def basic_disconnect(self, sid, namespace, **kwargs):
        if namespace not in self.rooms:
            return
        rooms = []
        for room_name, room in self.rooms[namespace].copy().items():
            if sid in room:
                rooms.append(room_name)
        for room in rooms:
            self.basic_leave_room(sid, namespace, room)
        # any acknowledgement that is still waiting on this client for the
        # disconnected namespace is removed; pending acks registered against
        # other namespaces of the same sid are left untouched
        if sid in self.callbacks and namespace in self.callbacks[sid]:
            pending = self.callbacks[sid].pop(namespace)
            if not self.callbacks[sid]:
                del self.callbacks[sid]
            for ack in pending.values():
                if isinstance(ack, Ack):
                    ack.disconnected()
        if namespace in self.pending_connect and \
                sid in self.pending_connect[namespace]:
            self.pending_connect[namespace].remove(sid)
            if len(self.pending_connect[namespace]) == 0:  # pragma: no branch
                del self.pending_connect[namespace]
        if namespace in self.pending_disconnect and \
                sid in self.pending_disconnect[namespace]:
            self.pending_disconnect[namespace].remove(sid)
            if len(self.pending_disconnect[namespace]) == 0:
                del self.pending_disconnect[namespace]

    def is_sid_room(self, namespace, room):
        return room in self.rooms.get(namespace, {}).get(None, {})

    def basic_enter_room(self, sid, namespace, room, eio_sid=None):
        if eio_sid is None and namespace not in self.rooms:
            raise ValueError('sid is not connected to requested namespace')
        if sid != room and self.is_sid_room(namespace, room):
            raise ValueError('cannot enter a sid room')
        if namespace not in self.rooms:
            self.rooms[namespace] = {}
        if room not in self.rooms[namespace]:
            self.rooms[namespace][room] = bidict()
        if eio_sid is None:
            eio_sid = self.rooms[namespace][None][sid]
        self.rooms[namespace][room][sid] = eio_sid

    def basic_leave_room(self, sid, namespace, room):
        try:
            del self.rooms[namespace][room][sid]
            if len(self.rooms[namespace][room]) == 0:
                del self.rooms[namespace][room]
                if len(self.rooms[namespace]) == 0:
                    del self.rooms[namespace]
        except KeyError:
            pass

    def basic_close_room(self, room, namespace):
        if self.is_sid_room(namespace, room):
            raise ValueError('cannot close a sid room')
        try:
            for sid, _ in self.get_participants(namespace, room):
                self.basic_leave_room(sid, namespace, room)
        except KeyError:  # pragma: no cover
            pass

    def get_rooms(self, sid, namespace):
        """Return the rooms a client is in."""
        r = []
        try:
            for room_name, room in self.rooms[namespace].items():
                if room_name is not None and sid in room:
                    r.append(room_name)
        except KeyError:
            pass
        return r

    def _generate_ack_id(self, sid, namespace, callback, on_disconnect=None):
        """Generate a unique identifier for an ACK packet.

        Callbacks are tracked per ``(sid, namespace)`` so that ACK ids only
        have to be unique within a namespace and disconnecting from one
        namespace never removes callbacks waiting on another namespace of the
        same connection. The returned object is an :class:`Ack` wrapper.
        """
        id = self._reserve_ack_id(sid, namespace)
        self._attach_ack(sid, namespace, id, callback, on_disconnect)
        return id

    def _reserve_ack_id(self, sid, namespace):
        """Reserve an ACK id without attaching a callback yet.

        Used by :meth:`Server.call`, which registers the entry before sending
        the packet so that a disconnect happening during the send can already
        be observed. The callback is attached later through
        :meth:`_attach_ack`.
        """
        namespace = namespace or '/'
        if sid not in self.callbacks:
            self.callbacks[sid] = {}
        if namespace not in self.callbacks[sid]:
            self.callbacks[sid][namespace] = {0: itertools.count(1)}
        return next(self.callbacks[sid][namespace][0])

    def _attach_ack(self, sid, namespace, id, callback, on_disconnect=None):
        """Store the callback for a previously reserved or new ACK id.

        If an entry already exists for ``id`` (the :meth:`Server.call`
        pre-registration case), it is reused instead of being overwritten, so
        the disconnect hook registered by the caller is preserved.
        """
        namespace = namespace or '/'
        registry = self.callbacks.setdefault(sid, {}).setdefault(
            namespace, {0: itertools.count(1)})
        if id not in registry:
            registry[id] = Ack(callback, on_disconnect)
        return id

    def _pop_ack(self, sid, namespace, id):
        """Atomically remove and return a pending ack, or ``None`` if it is
        no longer registered (already acknowledged, timed out or removed by a
        disconnect)."""
        namespace = namespace or '/'
        try:
            return self.callbacks[sid][namespace].pop(id)
        except KeyError:
            return None

    def _cancel_ack(self, sid, namespace, id):
        """Remove a pending ack without invoking its callback.

        Returns ``True`` if the ack was still registered and removed, and
        ``False`` if an acknowledgement won the race and resolved it first.
        """
        return self._pop_ack(sid, namespace, id) is not None

    def _pending_ack_count(self, sid=None, namespace=None):
        """Return the number of application callbacks currently registered.

        Introspection helper used to verify that no callback entries survive a
        completed, timed out, cancelled or disconnected call. The registries
        are snapshotted before iterating so the count stays consistent even
        when worker threads mutate them concurrently."""
        if sid is None:
            return sum(self._pending_ack_count(registered_sid, namespace)
                       for registered_sid in list(self.callbacks))
        registry_by_ns = self.callbacks.get(sid)
        if registry_by_ns is None:
            return 0
        if namespace is not None:
            registry = registry_by_ns.get(namespace or '/')
            return len([id for id in registry if id != 0]) \
                if registry is not None else 0
        return sum(
            len([id for id in registry if id != 0])
            for registry in list(registry_by_ns.values()))

    def _get_logger(self):
        """Get the appropriate logger

        Prevents uninitialized servers in write-only mode from failing.
        """

        if self.logger:
            return self.logger
        elif self.server:
            return self.server.logger
        else:
            return default_logger
