# Copyright 2021 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import argparse
import yaml
import time
import threading
import datetime

import rclpy
import rclpy.node
from rclpy.parameter import Parameter

# Note: this package is located at /opt/ros/humble/lib/python/site-packages, where
#       there's a file named "rmf_adapter.cpython-310-x86_64-linux-gnu.so"
import rmf_adapter as adpt
import rmf_adapter.vehicletraits as traits
import rmf_adapter.battery as battery
import rmf_adapter.geometry as geometry
import rmf_adapter.graph as graph
import rmf_adapter.plan as plan

from rmf_task_msgs.msg import TaskProfile, TaskType
from rmf_fleet_msgs.msg import LaneRequest, ClosedLanes

from functools import partial

from rclpy.qos import QoSProfile
from rclpy.qos import QoSHistoryPolicy as History
from rclpy.qos import QoSDurabilityPolicy as Durability
from rclpy.qos import QoSReliabilityPolicy as Reliability
from rclpy.qos import qos_profile_system_default

from .RobotCommandHandle import RobotCommandHandle
from .RobotClientAPI import RobotAPI

import logging
logging.basicConfig(
    filename='/home/jbao/rmf_ws/log/jerry_log/fleet_adapter.log', level=logging.INFO)
# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------


def initialize_fleet(config_yaml, nav_graph_path, node, use_sim_time):
    # Profile and traits
    fleet_config = config_yaml['rmf_fleet']
    # Note: doc: https://osrf.github.io/rmf_fleet_adapter_python/rmf_adapter.vehicletraits.html#Profile
    profile = traits.Profile(geometry.make_final_convex_circle(
        fleet_config['profile']['footprint']),
        geometry.make_final_convex_circle(fleet_config['profile']['vicinity']))
    # Note: doc: https://osrf.github.io/rmf_fleet_adapter_python/rmf_adapter.vehicletraits.html#VehicleTraits
    # Note: linear and angular velocity and accelration, in m/s, m/s^2, rad/s, rad/s^2 respectively
    vehicle_traits = traits.VehicleTraits(
        linear=traits.Limits(*fleet_config['limits']['linear']),
        angular=traits.Limits(*fleet_config['limits']['angular']),
        profile=profile)
    vehicle_traits.differential.reversible = fleet_config['reversible']

    # Battery system
    voltage = fleet_config['battery_system']['voltage']
    capacity = fleet_config['battery_system']['capacity']
    charging_current = fleet_config['battery_system']['charging_current']
    battery_sys = battery.BatterySystem.make(
        voltage, capacity, charging_current)

    # Mechanical system
    mass = fleet_config['mechanical_system']['mass']
    moment = fleet_config['mechanical_system']['moment_of_inertia']
    friction = fleet_config['mechanical_system']['friction_coefficient']
    mech_sys = battery.MechanicalSystem.make(mass, moment, friction)

    # Power systems
    ambient_power_sys = battery.PowerSystem.make(
        fleet_config['ambient_system']['power'])
    tool_power_sys = battery.PowerSystem.make(
        fleet_config['tool_system']['power'])

    # Power sinks
    # Note: for diff between Motion and Deive Power Sinks, see Notion note "fleet_adapter.py"
    motion_sink = battery.SimpleMotionPowerSink(battery_sys, mech_sys)
    ambient_sink = battery.SimpleDevicePowerSink(
        battery_sys, ambient_power_sys)
    tool_sink = battery.SimpleDevicePowerSink(battery_sys, tool_power_sys)

    nav_graph = graph.parse_graph(nav_graph_path, vehicle_traits)

    # Adapter
    fleet_name = fleet_config['name']
    adapter = adpt.Adapter.make(f'{fleet_name}_fleet_adapter')
    if use_sim_time:
        adapter.node.use_sim_time()
    assert adapter, ("Unable to initialize fleet adapter. Please ensure "
                     "RMF Schedule Node is running")
    adapter.start()
    time.sleep(1.0)

    node.declare_parameter('server_uri', rclpy.Parameter.Type.STRING)
    server_uri = node.get_parameter(
        'server_uri').get_parameter_value().string_value
    logging.info(f"server_uri: {server_uri}")
    if server_uri == "":
        server_uri = None

    # Note: adapter.add_fleet() returns a rmf_adapter.FleetUpdateHandle object
    fleet_handle = adapter.add_fleet(
        fleet_name, vehicle_traits, nav_graph, server_uri)

    fleet_state_update_frequency = fleet_config['publish_fleet_state']
    fleet_handle.fleet_state_publish_period(
        datetime.timedelta(seconds=1.0/fleet_state_update_frequency))
    # Account for battery drain
    drain_battery = fleet_config['account_for_battery_drain']
    lane_merge_distance = fleet_config.get('lane_merge_distance', 0.1)
    recharge_threshold = fleet_config['recharge_threshold']
    recharge_soc = fleet_config['recharge_soc']
    finishing_request = fleet_config['task_capabilities']['finishing_request']
    node.get_logger().info(f"Finishing request: [{finishing_request}]")
    # Set task planner params
    # Note: doc: https://openrmf.readthedocs.io/projects/rmf-ros2/en/latest/api/classrmf__fleet__adapter_1_1agv_1_1FleetUpdateHandle.html?highlight=set_task_planner_params#_CPPv4N17rmf_fleet_adapter3agv17FleetUpdateHandle23set_task_planner_paramsENSt10shared_ptrIN11rmf_battery3agv13BatterySystemEEENSt10shared_ptrIN11rmf_battery15MotionPowerSinkEEENSt10shared_ptrIN11rmf_battery15DevicePowerSinkEEENSt10shared_ptrIN11rmf_battery15DevicePowerSinkEEEddbN8rmf_task22ConstRequestFactoryPtrE
    ok = fleet_handle.set_task_planner_params(
        battery_sys,
        motion_sink,
        ambient_sink,
        tool_sink,
        recharge_threshold,
        recharge_soc,
        drain_battery,
        finishing_request)
    assert ok, ("Unable to set task planner params")

    task_capabilities = []
    if fleet_config['task_capabilities']['loop']:
        node.get_logger().info(
            f"Fleet [{fleet_name}] is configured to perform Loop tasks")
        task_capabilities.append(TaskType.TYPE_LOOP)
    if fleet_config['task_capabilities']['delivery']:
        node.get_logger().info(
            f"Fleet [{fleet_name}] is configured to perform Delivery tasks")
        task_capabilities.append(TaskType.TYPE_DELIVERY)
    if fleet_config['task_capabilities']['clean']:
        node.get_logger().info(
            f"Fleet [{fleet_name}] is configured to perform Clean tasks")
        task_capabilities.append(TaskType.TYPE_CLEAN)

    # Callable for validating requests that this fleet can accommodate
    def _task_request_check(task_capabilities, msg: TaskProfile):
        if msg.description.task_type in task_capabilities:
            return True
        else:
            return False

    # Note: partial() fixes the first param of _task_request_check to be task_capabilities
    #       defined above
    fleet_handle.accept_task_requests(
        partial(_task_request_check, task_capabilities))

    # Note: doc: https://docs.ros.org/en/humble/p/rmf_fleet_adapter/generated/classrmf__fleet__adapter_1_1agv_1_1FleetUpdateHandle_1_1Confirmation.html#exhale-class-classrmf-fleet-adapter-1-1agv-1-1fleetupdatehandle-1-1confirmation
    def _consider(description: dict):
        confirm = adpt.fleet_update_handle.Confirmation()
        confirm.accept()
        # returns a Confirmation object which accepts anything
        return confirm

    # Configure this fleet to perform any kind of teleop action
    fleet_handle.add_performable_action("teleop", _consider)

    def _updater_inserter(cmd_handle, update_handle):
        """Insert a RobotUpdateHandle."""
        cmd_handle.update_handle = update_handle

        def _action_executor(category: str,
                             description: dict,
                             execution:
                             adpt.robot_update_handle.ActionExecution):
            # Note: enter the lock; after execution is done inside this with block,
            #           lock is released
            #       Set the action_waypoint_index to the description one or the last
            #           known waypoint
            with cmd_handle._lock:
                if len(description) > 0 and\
                        description in cmd_handle.graph.keys:
                    cmd_handle.action_waypoint_index = \
                        cmd_handle.find_waypoint(description).index
                else:
                    cmd_handle.action_waypoint_index = \
                        cmd_handle.last_known_waypoint_index
                cmd_handle.on_waypoint = None
                cmd_handle.on_lane = None
                cmd_handle.action_execution = execution
        # Set the action_executioner for the robot
        cmd_handle.update_handle.set_action_executor(_action_executor)
        # Note: in robot var initialized below, config > robot_config > robots_config > config.yaml
        if ("max_delay" in cmd_handle.config.keys()):
            max_delay = cmd_handle.config["max_delay"]
            cmd_handle.node.get_logger().info(
                f"Setting max delay to {max_delay}s")
            cmd_handle.update_handle.set_maximum_delay(max_delay)
        # Note: if there's only 1 waypoint (i.e. charger), its index is 0
        if (cmd_handle.charger_waypoint_index <
                cmd_handle.graph.num_waypoints):
            cmd_handle.update_handle.set_charger_waypoint(
                cmd_handle.charger_waypoint_index)
        else:
            cmd_handle.node.get_logger().warn(
                "Invalid waypoint supplied for charger. "
                "Using default nearest charger in the map")

    # Initialize robot API for this fleet
    prefix = 'http://' + fleet_config['fleet_manager']['ip'] + \
             ':' + str(fleet_config['fleet_manager']['port'])
    api = RobotAPI(
        prefix,
        fleet_config['fleet_manager']['user'],
        fleet_config['fleet_manager']['password'])

    # Initialize robots for this fleet
    robots = {}
    # Note: initially, all robots are missing (i.e., not added to the fleet)
    missing_robots = config_yaml['robots']

    def _add_fleet_robots():
        while len(missing_robots) > 0:
            time.sleep(0.2)
            # Note: keys are robot names, e.g., tinyRobot1, tinyRobot2
            for robot_name in list(missing_robots.keys()):
                node.get_logger().debug(f"Connecting to robot: {robot_name}")
                # Note: Get HTTP Response for robot_name robot; data method is defined
                #           inside RobotClientAPI.py
                data = api.data(robot_name)
                if data is None:
                    continue
                if data['success']:
                    node.get_logger().info(f"Initializing robot: {robot_name}")
                    robots_config = config_yaml['robots'][robot_name]
                    rmf_config = robots_config['rmf_config']
                    robot_config = robots_config['robot_config']
                    initial_waypoint = rmf_config['start']['waypoint']
                    if 'position' in data['data']:
                        initial_orientation = data['data']['position']['yaw']
                    # Note: default orientation for the robot in the config file if there is no
                        #       position data in data['data']
                    else:
                        initial_orientation = \
                            rmf_config['start']['orientation']

                    starts = []
                    time_now = adapter.now()
                    # No need to offset robot and RMF crs for demos
                    position = api.position(robot_name)
                    # Note: cannot get initial position, cannot start robot
                    if position is None:
                        node.get_logger().info(
                            f'Failed to get initial position of {robot_name}'
                        )
                        continue

                    # Note: have both initial waypoint and initial orientation,
                    #           can start the robot with these 2 params
                    if (initial_waypoint is not None) and\
                            (initial_orientation is not None):
                        node.get_logger().info(
                            f"Using provided initial waypoint "
                            f"[{initial_waypoint}] "
                            f"and orientation [{initial_orientation:.2f}] to "
                            f"initialize starts for robot [{robot_name}]")
                        # Get the waypoint index for initial_waypoint
                        initial_waypoint_index = nav_graph.find_waypoint(
                            initial_waypoint).index
                        starts = [plan.Start(time_now,
                                             initial_waypoint_index,
                                             initial_orientation)]
                    # Note: if at least one of the initial waypoint and initial orientation
                        #       is missing
                    else:
                        node.get_logger().info(
                            f"Running compute_plan_starts for robot: "
                            "{robot_name}")
                        # Note: attempts to find the most suitable waypoints and lanes
                        #           in order to start planning
                        #       doc: https://docs.ros.org/en/ros2_packages/rolling/api/rmf_traffic/generated/function_Planner_8hpp_1a2fb0549d3d136e586b19b6952606a960.html
                        starts = plan.compute_plan_starts(
                            nav_graph,
                            rmf_config['start']['map_name'],
                            position,
                            time_now)

                    if starts is None or len(starts) == 0:
                        node.get_logger().error(
                            f"Unable to determine StartSet for {robot_name}")
                        continue

                    robot = RobotCommandHandle(
                        name=robot_name,
                        fleet_name=fleet_name,
                        config=robot_config,
                        node=node,
                        graph=nav_graph,
                        vehicle_traits=vehicle_traits,
                        map_name=rmf_config['start']['map_name'],
                        start=starts[0],
                        position=position,
                        charger_waypoint=rmf_config['charger']['waypoint'],
                        update_frequency=rmf_config.get(
                            'robot_state_update_frequency', 1),
                        lane_merge_distance=lane_merge_distance,
                        adapter=adapter,
                        api=api)

                    if robot.initialized:
                        robots[robot_name] = robot
                        # Add robot to fleet
                        fleet_handle.add_robot(robot,
                                               robot_name,
                                               profile,
                                               [starts[0]],
                                               partial(_updater_inserter,
                                                       robot))
                        node.get_logger().info(
                            f"Successfully added new robot: {robot_name}")

                    else:
                        node.get_logger().error(
                            f"Failed to initialize robot: {robot_name}")

                    # Note: robot initialized, remove from missing robot list
                    del missing_robots[robot_name]

                else:
                    pass
                    node.get_logger().debug(
                        f"{robot_name} not found, trying again...")
        return

    # Note: initialize the thread to add robots. This thread runs separate from the
    #           main thread
    add_robots = threading.Thread(target=_add_fleet_robots, args=())
    add_robots.start()

    # Add LaneRequest callback
    closed_lanes = []

    def _lane_request_cb(msg):
        if msg.fleet_name is None or msg.fleet_name != fleet_name:
            return

        # Note: on the fleet level, specify a set of lanes that should be open/closed
        #           doc: https://docs.ros.org/en/humble/p/rmf_fleet_adapter/generated/classrmf__fleet__adapter_1_1agv_1_1FleetUpdateHandle.html#exhale-class-classrmf-fleet-adapter-1-1agv-1-1fleetupdatehandle
        fleet_handle.open_lanes(msg.open_lanes)
        fleet_handle.close_lanes(msg.close_lanes)

        newly_closed_lanes = []

        # Note: add newly closed lanes to closed_lanes and newly_closed_lanes
        for lane_idx in msg.close_lanes:
            if lane_idx not in closed_lanes:
                newly_closed_lanes.append(lane_idx)
                closed_lanes.append(lane_idx)
        # Note: some lanes in closed_lanes might be opened
        for lane_idx in msg.open_lanes:
            if lane_idx in closed_lanes:
                closed_lanes.remove(lane_idx)

        # Note: on the robot level, add the newly closed lanes
        for robot_name, robot in robots.items():
            robot.newly_closed_lanes(newly_closed_lanes)

        state_msg = ClosedLanes()
        state_msg.fleet_name = fleet_name
        state_msg.closed_lanes = closed_lanes
        closed_lanes_pub.publish(state_msg)

    transient_qos = QoSProfile(
        history=History.KEEP_LAST,
        depth=1,
        reliability=Reliability.RELIABLE,
        durability=Durability.TRANSIENT_LOCAL)

    # Note: params: message type, topic name, callback, quality of service proifle
    #       doc: https://docs.ros2.org/foxy/api/rclpy/api/node.html
    node.create_subscription(
        LaneRequest,
        'lane_closure_requests',
        _lane_request_cb,
        qos_profile=qos_profile_system_default)

    closed_lanes_pub = node.create_publisher(
        ClosedLanes,
        'closed_lanes',
        qos_profile=transient_qos)

    return adapter


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main(argv=sys.argv):
    logging.INFO("In the main")
    # Init rclpy and adapter
    rclpy.init(args=argv)
    adpt.init_rclcpp()
    # Note: return a list of only the non-ROS command line arguments
    #       doc: https://docs.ros.org/en/iron/p/rclpy/rclpy.utilities.html
    args_without_ros = rclpy.utilities.remove_ros_args(argv)
    # Note: log what are the args_without_ros
    logging.info(f"args_without_ros: {args_without_ros}")

    parser = argparse.ArgumentParser(
        prog="fleet_adapter",
        description="Configure and spin up the fleet adapter")
    # Note: check the log file and -c, -n, -sim will make sense
    parser.add_argument("-c", "--config_file", type=str, required=True,
                        help="Path to the config.yaml file")
    parser.add_argument("-n", "--nav_graph", type=str, required=True,
                        help="Path to the nav_graph for this fleet adapter")
    parser.add_argument("-sim", "--use_sim_time", action="store_true",
                        help='Use sim time, default: false')
    args = parser.parse_args(args_without_ros[1:])
    print(f"Starting fleet adapter...")

    config_path = args.config_file
    nav_graph_path = args.nav_graph

    # Load config and nav graph yamls
    with open(config_path, "r") as f:
        config_yaml = yaml.safe_load(f)

    # ROS 2 node for the command handle
    fleet_name = config_yaml['rmf_fleet']['name']
    node = rclpy.node.Node(f'{fleet_name}_command_handle')

    # Enable sim time for testing offline
    if args.use_sim_time:
        param = Parameter("use_sim_time", Parameter.Type.BOOL, True)
        node.set_parameters([param])

    # Note: this seems useless as adapter was never used; commented it out but the demo still ran
    adapter = initialize_fleet(
        config_yaml,
        nav_graph_path,
        node,
        args.use_sim_time)

    # Create executor for the command handle node
    rclpy_executor = rclpy.executors.SingleThreadedExecutor()
    rclpy_executor.add_node(node)

    # Start the fleet adapter
    rclpy_executor.spin()

    # Shutdown
    node.destroy_node()
    rclpy_executor.shutdown()
    rclpy.shutdown()


if __name__ == '__main__':
    main(sys.argv)
