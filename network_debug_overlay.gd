class_name NetworkDebugOverlay
extends CanvasLayer

## Reusable runtime network overlay for P2PNetworkManager.
##
## The script can also be attached to a Control node. In that case, change
## the base class below from CanvasLayer to Control and keep the same child
## layout.

@export var manager_path: NodePath
@export var manager_group := "p2p_network_manager"
@export var show_totals := false

@onready var panel: Control = $Panel
@onready var ping_label: Label = $Panel/MarginContainer/VBoxContainer/PingLabel
@onready var sent_label: Label = $Panel/MarginContainer/VBoxContainer/SentLabel
@onready var received_label: Label = $Panel/MarginContainer/VBoxContainer/ReceivedLabel
@onready var connection_label: Label = $Panel/MarginContainer/VBoxContainer/ConnectionLabel

var manager: Node
var previous_bytes_sent := 0
var previous_bytes_received := 0
var previous_sample_time := 0.0
var warned_about_manager := false


func _ready() -> void:
	_validate_labels()
	manager = _find_manager()
	previous_sample_time = Time.get_ticks_usec() / 1_000_000.0

	if manager == null:
		_set_disconnected_state()


func _process(_delta: float) -> void:
	if not is_instance_valid(manager):
		manager = _find_manager()
		if manager == null:
			_set_disconnected_state()
			return

	if not manager.has_method("get_network_metrics"):
		if not warned_about_manager:
			push_warning("NetworkDebugOverlay manager has no get_network_metrics() method.")
			warned_about_manager = true
		_set_disconnected_state()
		return

	var metrics: Dictionary = manager.get_network_metrics()
	_update_metrics(metrics)


func _find_manager() -> Node:
	# An explicit path is preferred because it avoids ambiguity in large scenes.
	if not manager_path.is_empty():
		var configured_manager := get_node_or_null(manager_path)
		if configured_manager != null:
			return configured_manager
		push_warning("NetworkDebugOverlay manager_path does not resolve: %s" % manager_path)

	# Groups are robust across scene renames and instancing.
	for candidate in get_tree().get_nodes_in_group(manager_group):
		if candidate is Node and candidate.has_method("get_network_metrics"):
			return candidate

	# Finally search registered native classes, including the Rust class.
	var root := get_tree().current_scene
	if root == null:
		root = get_tree().root
	var native_manager := _find_manager_recursive(root)
	if native_manager != null:
		return native_manager

	if not warned_about_manager:
		push_warning(
			"NetworkDebugOverlay could not find P2PNetworkManager. "
			+ "Set manager_path or add the manager to the '%s' group." % manager_group
		)
		warned_about_manager = true
	return null


func _find_manager_recursive(node: Node) -> Node:
	if node.has_method("get_network_metrics") and node.is_class("P2PNetworkManager"):
		return node
	for child in node.get_children():
		var result := _find_manager_recursive(child)
		if result != null:
			return result
	return null


func _update_metrics(metrics: Dictionary) -> void:
	var ping_ms := float(metrics.get("ping_ms", 0))
	var bytes_sent := int(metrics.get("bytes_sent", 0))
	var bytes_received := int(metrics.get("bytes_received", 0))
	var connection_type := str(metrics.get("connection_type", "Disconnected"))

	var now := Time.get_ticks_usec() / 1_000_000.0
	var elapsed := maxf(now - previous_sample_time, 0.001)
	var sent_rate := maxf(float(bytes_sent - previous_bytes_sent), 0.0) / elapsed
	var received_rate := maxf(float(bytes_received - previous_bytes_received), 0.0) / elapsed

	ping_label.text = "Ping: %d ms" % roundi(ping_ms)
	if show_totals:
		sent_label.text = "Sent: %s (%s/s)" % [
			_format_bytes(bytes_sent),
			_format_bytes(sent_rate),
		]
		received_label.text = "Received: %s (%s/s)" % [
			_format_bytes(bytes_received),
			_format_bytes(received_rate),
		]
	else:
		sent_label.text = "Sent: %s/s" % _format_bytes(sent_rate)
		received_label.text = "Received: %s/s" % _format_bytes(received_rate)

	connection_label.text = "Route: %s" % connection_type
	connection_label.modulate = _connection_color(connection_type)

	previous_bytes_sent = bytes_sent
	previous_bytes_received = bytes_received
	previous_sample_time = now


func _format_bytes(value: float) -> String:
	if value < 1024.0:
		return "%.0f B" % value
	if value < 1024.0 * 1024.0:
		return "%.1f KiB" % (value / 1024.0)
	if value < 1024.0 * 1024.0 * 1024.0:
		return "%.1f MiB" % (value / (1024.0 * 1024.0))
	return "%.1f GiB" % (value / (1024.0 * 1024.0 * 1024.0))


func _connection_color(connection_type: String) -> Color:
	match connection_type:
		"Direct":
			return Color("66e08a")
		"Relayed":
			return Color("f2c94c")
		"Disconnected":
			return Color("eb5757")
		_:
			return Color("bdbdbd")


func _set_disconnected_state() -> void:
	ping_label.text = "Ping: --"
	sent_label.text = "Sent: --"
	received_label.text = "Received: --"
	connection_label.text = "Route: Disconnected"
	connection_label.modulate = _connection_color("Disconnected")


func _validate_labels() -> void:
	var labels := [ping_label, sent_label, received_label, connection_label]
	for label in labels:
		if label == null:
			push_warning(
				"NetworkDebugOverlay expects PingLabel, SentLabel, "
				+ "ReceivedLabel, and ConnectionLabel under Panel/MarginContainer/VBoxContainer."
			)
