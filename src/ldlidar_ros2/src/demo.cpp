/**
 * @file main.cpp
 * @author LDRobot (contact@ldrobot.com)
 * @brief  main process App
 *         This code is only applicable to LDROBOT LiDAR LD00 LD03 LD08 LD14
 * products sold by Shenzhen LDROBOT Co., LTD
 * @version 0.1
 * @date 2021-11-10
 *
 * @copyright Copyright (c) 2021  SHENZHEN LDROBOT CO., LTD. All rights
 * reserved.
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the file LICENSE
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ros2_api.h"
#include "ldlidar_driver/ldlidar_driver_linux.h"
#include "rcl_interfaces/msg/parameter_descriptor.hpp"
#include "rcl_interfaces/msg/integer_range.hpp"
#include "rcl_interfaces/msg/floating_point_range.hpp"
#include <thread>
#include <rclcpp/executors/multi_threaded_executor.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>
#include <fstream>

uint64_t GetTimestamp(void);


void  ToLaserscanMessagePublish(ldlidar::Points2D& src,  double lidar_spin_freq, LaserScanSetting& setting,
  rclcpp::Node::SharedPtr& node, rclcpp::Publisher<sensor_msgs::msg::LaserScan>::SharedPtr& lidarpub);

void  ToSensorPointCloudMessagePublish(ldlidar::Points2D& src, LaserScanSetting& setting,
  rclcpp::Node::SharedPtr& node, rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr& lidarpub);

int main(int argc, char **argv) {
  rclcpp::init(argc, argv);

  // create a ROS2 Node
  auto node = std::make_shared<rclcpp::Node>("ldlidar_publisher_ld19"); 

  // Initialize variables
  std::string product_name;
  std::string laser_scan_topic_name;
  std::string point_cloud_2d_topic_name;
  std::string port_name;
  int serial_baudrate;
  ldlidar::LDType lidartypename;

  // Initialize default settings
  LaserScanSetting setting;
  setting.frame_id = "ld19_frame";
  setting.laser_scan_dir = true;
  setting.enable_angle_crop_func = false;
  setting.angle_crop_min = 135.0;
  setting.angle_crop_max = 225.0;
  setting.range_min = 0.02;
  setting.range_max = 12.0;

  // declare ros2 params with descriptions and constraints
  rcl_interfaces::msg::ParameterDescriptor product_name_desc;
  product_name_desc.description = "LiDAR product model name";
  product_name_desc.read_only = true;
  node->declare_parameter("product_name", "LDLiDAR_LD19", product_name_desc);

  rcl_interfaces::msg::ParameterDescriptor laser_scan_desc;
  laser_scan_desc.description = "Topic name for LaserScan messages";
  node->declare_parameter("laser_scan_topic_name", "scan", laser_scan_desc);

  rcl_interfaces::msg::ParameterDescriptor pointcloud_desc;
  pointcloud_desc.description = "Topic name for PointCloud2D messages";
  node->declare_parameter("point_cloud_2d_topic_name", "pointcloud2d", pointcloud_desc);

  rcl_interfaces::msg::ParameterDescriptor frame_desc;
  frame_desc.description = "TF frame ID for LiDAR messages";
  node->declare_parameter("frame_id", setting.frame_id, frame_desc);

  rcl_interfaces::msg::ParameterDescriptor port_desc;
  port_desc.description = "Serial port device name";
  node->declare_parameter("port_name", "/dev/ttyTHS1", port_desc);

  rcl_interfaces::msg::ParameterDescriptor baudrate_desc;
  baudrate_desc.description = "Serial port baud rate";
  rcl_interfaces::msg::IntegerRange baudrate_range;
  baudrate_range.from_value = 115200;
  baudrate_range.to_value = 921600;
  baudrate_range.step = 115200;
  baudrate_desc.integer_range.push_back(baudrate_range);
  node->declare_parameter("serial_baudrate", 230400, baudrate_desc);

  rcl_interfaces::msg::ParameterDescriptor scan_dir_desc;
  scan_dir_desc.description = "Laser scan direction (true for counterclockwise, false for clockwise)";
  node->declare_parameter("laser_scan_dir", setting.laser_scan_dir, scan_dir_desc);

  rcl_interfaces::msg::ParameterDescriptor crop_func_desc;
  crop_func_desc.description = "Enable angle cropping function";
  node->declare_parameter("enable_angle_crop_func", setting.enable_angle_crop_func, crop_func_desc);

  rcl_interfaces::msg::ParameterDescriptor angle_min_desc;
  angle_min_desc.description = "Minimum angle for cropping (degrees)";
  rcl_interfaces::msg::FloatingPointRange angle_min_range;
  angle_min_range.from_value = 0.0;
  angle_min_range.to_value = 360.0;
  angle_min_desc.floating_point_range.push_back(angle_min_range);
  node->declare_parameter("angle_crop_min", setting.angle_crop_min, angle_min_desc);

  rcl_interfaces::msg::ParameterDescriptor angle_max_desc;
  angle_max_desc.description = "Maximum angle for cropping (degrees)";
  rcl_interfaces::msg::FloatingPointRange angle_max_range;
  angle_max_range.from_value = 0.0;
  angle_max_range.to_value = 360.0;
  angle_max_desc.floating_point_range.push_back(angle_max_range);
  node->declare_parameter("angle_crop_max", setting.angle_crop_max, angle_max_desc);

  rcl_interfaces::msg::ParameterDescriptor range_min_desc;
  range_min_desc.description = "Minimum range value (meters)";
  rcl_interfaces::msg::FloatingPointRange range_min_range;
  range_min_range.from_value = 0.0;
  range_min_range.to_value = 1.0;
  range_min_desc.floating_point_range.push_back(range_min_range);
  node->declare_parameter("range_min", setting.range_min, range_min_desc);

  rcl_interfaces::msg::ParameterDescriptor range_max_desc;
  range_max_desc.description = "Maximum range value (meters)";
  rcl_interfaces::msg::FloatingPointRange range_max_range;
  range_max_range.from_value = 1.0;
  range_max_range.to_value = 30.0;
  range_max_desc.floating_point_range.push_back(range_max_range);
  node->declare_parameter("range_max", setting.range_max, range_max_desc);

  // get ros2 param
  node->get_parameter("product_name", product_name);
  node->get_parameter("laser_scan_topic_name", laser_scan_topic_name);
  node->get_parameter("point_cloud_2d_topic_name", point_cloud_2d_topic_name);
  node->get_parameter("frame_id", setting.frame_id);
  node->get_parameter("port_name", port_name);
  node->get_parameter("serial_baudrate", serial_baudrate);
  node->get_parameter("laser_scan_dir", setting.laser_scan_dir);
  node->get_parameter("enable_angle_crop_func", setting.enable_angle_crop_func);
  node->get_parameter("angle_crop_min", setting.angle_crop_min);
  node->get_parameter("angle_crop_max", setting.angle_crop_max);
  node->get_parameter("range_min", setting.range_min);
  node->get_parameter("range_max", setting.range_max);

  ldlidar::LDLidarDriverLinuxInterface* ldlidar_drv = 
    ldlidar::LDLidarDriverLinuxInterface::Create();

  RCLCPP_INFO(node->get_logger(), "LDLiDAR SDK Pack Version is:%s", ldlidar_drv->GetLidarSdkVersionNumber().c_str());
  RCLCPP_INFO(node->get_logger(), "ROS2 param input:");
  RCLCPP_INFO(node->get_logger(), "<product_name>: %s", product_name.c_str());
  RCLCPP_INFO(node->get_logger(), "<laser_scan_topic_name>: %s", laser_scan_topic_name.c_str());
  RCLCPP_INFO(node->get_logger(), "<point_cloud_2d_topic_name>: %s", point_cloud_2d_topic_name.c_str());
  RCLCPP_INFO(node->get_logger(), "<frame_id>: %s", setting.frame_id.c_str());
  RCLCPP_INFO(node->get_logger(), "<port_name>: %s ", port_name.c_str());
  RCLCPP_INFO(node->get_logger(), "<serial_baudrate>: %d ", serial_baudrate);
  RCLCPP_INFO(node->get_logger(), "<laser_scan_dir>: %s", (setting.laser_scan_dir?"Counterclockwise":"Clockwise"));
  RCLCPP_INFO(node->get_logger(), "<enable_angle_crop_func>: %s", (setting.enable_angle_crop_func?"true":"false"));
  RCLCPP_INFO(node->get_logger(), "<angle_crop_min>: %f", setting.angle_crop_min);
  RCLCPP_INFO(node->get_logger(), "<angle_crop_max>: %f", setting.angle_crop_max);
  RCLCPP_INFO(node->get_logger(), "<range_min>: %f", setting.range_min);
  RCLCPP_INFO(node->get_logger(), "<range_max>: %f", setting.range_max);

  if (port_name.empty()) {
    RCLCPP_ERROR(node->get_logger(), "fail, port_name is empty! Will keep trying...");
    // Don't exit - keep the node alive and try again
  }

  ldlidar_drv->RegisterGetTimestampFunctional(std::bind(&GetTimestamp)); 

  ldlidar_drv->EnablePointCloudDataFilter(true);
  
  if(!strcmp(product_name.c_str(),"LDLiDAR_LD14")) {
    lidartypename = ldlidar::LDType::LD_14;
  } else if (!strcmp(product_name.c_str(), "LDLiDAR_LD14P")) {
    lidartypename = ldlidar::LDType::LD_14P;
  } else if (!strcmp(product_name.c_str(),"LDLiDAR_LD06")) {
    lidartypename = ldlidar::LDType::LD_06;
  } else if (!strcmp(product_name.c_str(),"LDLiDAR_LD19")) {
    lidartypename = ldlidar::LDType::LD_19;
  } else {
    RCLCPP_ERROR(node->get_logger(),"Error, input param <product_name> is fail!! Will keep trying...");
    // Don't exit - keep the node alive and try again
  }

  if (ldlidar_drv->Connect(lidartypename, port_name, serial_baudrate)) {
    RCLCPP_INFO(node->get_logger(), "ldlidar serial connect is success");
  } else {
    RCLCPP_ERROR(node->get_logger(), "ldlidar serial connect is fail - will keep trying to reconnect");
    // Don't exit - will retry in main loop
  }

  if (ldlidar_drv->WaitLidarComm(3500)) {
    RCLCPP_INFO(node->get_logger(), "ldlidar communication is normal.");
  } else {
    RCLCPP_ERROR(node->get_logger(), "ldlidar communication is abnormal - will keep trying to reconnect");
    // Don't exit - will retry in main loop
  }

  if (ldlidar_drv->Start()) {
    RCLCPP_INFO(node->get_logger(), "ldlidar driver start is success.");
  } else {
    RCLCPP_ERROR(node->get_logger(), "ldlidar driver start is fail - will keep trying to reconnect");
    // Don't exit - will retry in main loop
  }

  // create ldlidar data topic and publisher
  rclcpp::Publisher<sensor_msgs::msg::LaserScan>::SharedPtr lidar_pub_laserscan = 
      node->create_publisher<sensor_msgs::msg::LaserScan>(laser_scan_topic_name, 10);
  
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_pub_pointcloud = 
      node->create_publisher<sensor_msgs::msg::PointCloud2>(point_cloud_2d_topic_name, 10);

  // Create a multithreaded executor for parameter services
  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(node);
  std::thread executor_thread([&executor]() { executor.spin(); });

  rclcpp::WallRate r(10); //Hz

  ldlidar::Points2D laser_scan_points;
  
  // Add reconnection variables
  int timeout_count = 0;
  const int max_timeout_count = 5; // 0.5 seconds at 10Hz - much faster reconnection
  bool needs_reconnection = false;
  bool initial_connection_failed = false;

  RCLCPP_INFO(node->get_logger(), "start normal, pub lidar data");

  // Check if initial connection failed and trigger reconnection
  if (port_name.empty() || 
      !ldlidar_drv->Connect(lidartypename, port_name, serial_baudrate) ||
      !ldlidar_drv->WaitLidarComm(3500) ||
      !ldlidar_drv->Start()) {
    RCLCPP_WARN(node->get_logger(), "Initial connection failed, will retry in main loop");
    initial_connection_failed = true;
    needs_reconnection = true;
  }

  while (rclcpp::ok()) {
  
    // Add simple check for driver validity before accessing data
    if (ldlidar_drv == nullptr) {
      RCLCPP_ERROR(node->get_logger(), "Driver is null, will keep trying to reconnect");
      needs_reconnection = true;
    } else if (!needs_reconnection && !initial_connection_failed) {
      // Only try to get data if we're not in reconnection mode
      // Add try-catch to handle reconnect crashes
      try {
        switch (ldlidar_drv->GetLaserScanData(laser_scan_points, 1500)){
          case ldlidar::LidarStatus::NORMAL: {
            // Reset timeout counter on successful data
            timeout_count = 0;
            initial_connection_failed = false; // Reset initial failure flag
            
            // Add simple data validation
            if (laser_scan_points.empty()) {
              RCLCPP_WARN(node->get_logger(), "Empty laser scan data received, skipping");
              break;
            }
            
            double lidar_scan_freq = 0;
            ldlidar_drv->GetLidarScanFreq(lidar_scan_freq);
            ToLaserscanMessagePublish(laser_scan_points, lidar_scan_freq, setting, node, lidar_pub_laserscan);
            ToSensorPointCloudMessagePublish(laser_scan_points, setting, node, lidar_pub_pointcloud);
            break;
          }
          case ldlidar::LidarStatus::DATA_TIME_OUT: {
            timeout_count++;
            RCLCPP_WARN(node->get_logger(), "ldlidar point cloud data publish time out (%d/%d), checking connection...", 
                       timeout_count, max_timeout_count);
            
            if (timeout_count >= max_timeout_count) {
              RCLCPP_ERROR(node->get_logger(), "Too many timeouts, attempting reconnection...");
              needs_reconnection = true;
              timeout_count = 0;
            }
            break;
          }
          case ldlidar::LidarStatus::DATA_WAIT: {
            break;
          }
          default:
            break;
        }
      } catch (const std::exception& e) {
        RCLCPP_ERROR(node->get_logger(), "Exception in main loop (likely USB reconnect issue): %s", e.what());
        needs_reconnection = true;
        // Add small delay to prevent rapid error loops
        rclcpp::sleep_for(std::chrono::milliseconds(100));
      } catch (...) {
        RCLCPP_ERROR(node->get_logger(), "Unknown exception in main loop (likely USB reconnect issue)");
        needs_reconnection = true;
        // Add small delay to prevent rapid error loops
        rclcpp::sleep_for(std::chrono::milliseconds(100));
      }
    }
    
    // Handle reconnection if needed
    if (needs_reconnection) {
      static int reconnect_attempts = 0;
      reconnect_attempts++;
      
      RCLCPP_INFO(node->get_logger(), "Attempting to reconnect to LiDAR (attempt %d)...", reconnect_attempts);
      
      // Stop and disconnect current driver
      ldlidar_drv->Stop();
      ldlidar_drv->Disconnect();
      
      // Wait shorter for USB to stabilize - faster reconnection
      rclcpp::sleep_for(std::chrono::milliseconds(500));
      
      // Check if the specified device exists
      std::ifstream device_file(port_name);
      if (!device_file.good()) {
        RCLCPP_WARN(node->get_logger(), "Specified device %s not found, waiting for USB device...", port_name.c_str());
        device_file.close();
        rclcpp::sleep_for(std::chrono::milliseconds(200));
        continue; // Skip this iteration and try again
      }
      device_file.close();
      
      // Try to reconnect using ONLY the specified device
      if (ldlidar_drv->Connect(lidartypename, port_name, serial_baudrate)) {
        RCLCPP_INFO(node->get_logger(), "LiDAR reconnection successful");
        
        // Wait for communication to establish - faster timeout
        if (ldlidar_drv->WaitLidarComm(1000)) {
          RCLCPP_INFO(node->get_logger(), "LiDAR communication re-established");
          
          // Start the driver again
          if (ldlidar_drv->Start()) {
            RCLCPP_INFO(node->get_logger(), "LiDAR driver restarted successfully");
            needs_reconnection = false;
            timeout_count = 0;
            reconnect_attempts = 0; // Reset attempt counter on success
          } else {
            RCLCPP_ERROR(node->get_logger(), "Failed to restart LiDAR driver, will retry...");
          }
        } else {
          RCLCPP_ERROR(node->get_logger(), "LiDAR communication failed after reconnection, will retry...");
        }
      } else {
        RCLCPP_ERROR(node->get_logger(), "LiDAR reconnection failed (attempt %d), will retry...", reconnect_attempts);
      }
      
      // Add shorter delay between reconnection attempts - faster retry
      rclcpp::sleep_for(std::chrono::milliseconds(200));
    }

    r.sleep();
  }

  ldlidar_drv->Stop();
  ldlidar_drv->Disconnect();

  ldlidar::LDLidarDriverLinuxInterface::Destory(ldlidar_drv);

  // Stop the executor and wait for the thread to finish
  executor.cancel();
  if (executor_thread.joinable()) {
    executor_thread.join();
  }

  RCLCPP_INFO(node->get_logger(), "ldlidar published is end");
  rclcpp::shutdown();

  return 0;
}

uint64_t GetTimestamp(void) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp = 
    std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now());
  auto tmp = std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch());
  return ((uint64_t)tmp.count());
}

void  ToLaserscanMessagePublish(ldlidar::Points2D& src,  double lidar_spin_freq, LaserScanSetting& setting,
  rclcpp::Node::SharedPtr& node, rclcpp::Publisher<sensor_msgs::msg::LaserScan>::SharedPtr& lidarpub) {
  float angle_min, angle_max, range_min, range_max, angle_increment;
  double scan_time;
  rclcpp::Time start_scan_time;
  static rclcpp::Time end_scan_time;
  static bool first_scan = true;

  start_scan_time = node->now();
  scan_time = (start_scan_time.seconds() - end_scan_time.seconds());

  if (first_scan) {
    first_scan = false;
    end_scan_time = start_scan_time;
    return;
  }
  // Adjust the parameters according to the demand
  angle_min = 0;
  angle_max = (2 * M_PI);
  range_min = static_cast<float>(setting.range_min);
  range_max = static_cast<float>(setting.range_max);
  int beam_size = static_cast<int>(src.size());
  if (beam_size <= 1) {
    end_scan_time = start_scan_time;
    RCLCPP_ERROR(node->get_logger(), "beam_size <= 1");
    return;
  }
  angle_increment = (angle_max - angle_min) / (float)(beam_size -1);
  // Calculate the number of scanning points
  if (lidar_spin_freq > 0) {
    sensor_msgs::msg::LaserScan output;
    output.header.stamp = start_scan_time;
    output.header.frame_id = setting.frame_id;
    output.angle_min = angle_min;
    output.angle_max = angle_max;
    output.range_min = range_min;
    output.range_max = range_max;
    output.angle_increment = angle_increment;
    if (beam_size <= 1) {
      output.time_increment = 0;
    } else {
      output.time_increment = static_cast<float>(scan_time / (double)(beam_size - 1));
    }
    output.scan_time = scan_time;
    // First fill all the data with Nan
    output.ranges.assign(beam_size, std::numeric_limits<float>::quiet_NaN());
    output.intensities.assign(beam_size, std::numeric_limits<float>::quiet_NaN());
    for (auto point : src) {
      // Add simple bounds checking to prevent crashes on corrupted data
      if (point.distance < 0 || point.distance > 50000 || 
          point.intensity < 0 || point.intensity > 1000 ||
          point.angle < 0 || point.angle > 360) {
        continue; // Skip invalid points that might cause crashes
      }
      
      float range = point.distance / 1000.f;  // distance unit transform to meters
      float intensity = point.intensity;      // laser receive intensity 
      float dir_angle = point.angle;

      if ((point.distance == 0) && (point.intensity == 0)) { // filter is handled to  0, Nan will be assigned variable.
        range = std::numeric_limits<float>::quiet_NaN(); 
        intensity = std::numeric_limits<float>::quiet_NaN();
      }

      if (setting.enable_angle_crop_func) { // Angle crop setting, Mask data within the set angle range
        if ((dir_angle >= setting.angle_crop_min) && (dir_angle <= setting.angle_crop_max)) {
          range = std::numeric_limits<float>::quiet_NaN();
          intensity = std::numeric_limits<float>::quiet_NaN();
        }
      }

      float angle = ANGLE_TO_RADIAN(dir_angle); // Lidar angle unit form degree transform to radian
      int index = static_cast<int>(ceil((angle - angle_min) / angle_increment));
      if (index < beam_size) {
        if (index < 0) {
          RCLCPP_ERROR(node->get_logger(), "error index: %d, beam_size: %d, angle: %f, output.angle_min: %f, output.angle_increment: %f", 
            index, beam_size, angle, angle_min, angle_increment);
        }

        if (setting.laser_scan_dir) {
          int index_anticlockwise = beam_size - index - 1;
          // If the current content is Nan, it is assigned directly
          if (std::isnan(output.ranges[index_anticlockwise])) {
            output.ranges[index_anticlockwise] = range;
          } else { // Otherwise, only when the distance is less than the current
                    //   value, it can be re assigned
            if (range < output.ranges[index_anticlockwise]) {
                output.ranges[index_anticlockwise] = range;
            }
          }
          output.intensities[index_anticlockwise] = intensity;
        } else {
          // If the current content is Nan, it is assigned directly
          if (std::isnan(output.ranges[index])) {
            output.ranges[index] = range;
          } else { // Otherwise, only when the distance is less than the current
                  //   value, it can be re assigned
            if (range < output.ranges[index]) {
              output.ranges[index] = range;
            }
          }
          output.intensities[index] = intensity;
        }
      }
    }
    lidarpub->publish(output);
    end_scan_time = start_scan_time;
  } 
}

void ToSensorPointCloudMessagePublish(ldlidar::Points2D& src, LaserScanSetting& setting,
  rclcpp::Node::SharedPtr& node, rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr& lidarpub) {
  
  rclcpp::Time start_scan_time;
  static rclcpp::Time end_scan_time;
  static bool first_scan = true;

  ldlidar::Points2D dst = src;

  start_scan_time = node->now();

  if (first_scan) {
    first_scan = false;
    end_scan_time = start_scan_time;
    return;
  }

  if (setting.laser_scan_dir) {
    for (auto&point : dst) {
      point.angle = 360.f - point.angle;
      if (point.angle < 0) {
        point.angle += 360.f;
      }
    }
  } 

  int frame_points_num = static_cast<int>(dst.size());

  sensor_msgs::msg::PointCloud2 output;
  output.header.stamp = start_scan_time;
  output.header.frame_id = setting.frame_id;

  // Set up the PointCloud2 message fields
  output.height = 1;
  output.width = frame_points_num;
  output.is_dense = true;
  output.is_bigendian = false;

  // Define the point cloud fields (x, y, z, intensity)
  output.fields.resize(4);
  output.fields[0].name = "x";
  output.fields[0].offset = 0;
  output.fields[0].datatype = sensor_msgs::msg::PointField::FLOAT32;
  output.fields[0].count = 1;
  output.fields[1].name = "y";
  output.fields[1].offset = 4;
  output.fields[1].datatype = sensor_msgs::msg::PointField::FLOAT32;
  output.fields[1].count = 1;
  output.fields[2].name = "z";
  output.fields[2].offset = 8;
  output.fields[2].datatype = sensor_msgs::msg::PointField::FLOAT32;
  output.fields[2].count = 1;
  output.fields[3].name = "intensity";
  output.fields[3].offset = 12;
  output.fields[3].datatype = sensor_msgs::msg::PointField::FLOAT32;
  output.fields[3].count = 1;

  output.point_step = 16;  // 4 fields * 4 bytes each
  output.row_step = output.point_step * output.width;
  output.data.resize(output.row_step);

  // Create iterators for filling the PointCloud2 data
  sensor_msgs::PointCloud2Iterator<float> iter_x(output, "x");
  sensor_msgs::PointCloud2Iterator<float> iter_y(output, "y");
  sensor_msgs::PointCloud2Iterator<float> iter_z(output, "z");
  sensor_msgs::PointCloud2Iterator<float> iter_intensity(output, "intensity");

  for (int i = 0; i < frame_points_num; ++i, ++iter_x, ++iter_y, ++iter_z, ++iter_intensity) {
    // Add simple bounds checking to prevent crashes on corrupted data
    if (dst[i].distance < 0 || dst[i].distance > 50000 || 
        dst[i].intensity < 0 || dst[i].intensity > 1000 ||
        dst[i].angle < 0 || dst[i].angle > 360) {
      // Set invalid points to NaN instead of crashing
      *iter_x = std::numeric_limits<float>::quiet_NaN();
      *iter_y = std::numeric_limits<float>::quiet_NaN();
      *iter_z = std::numeric_limits<float>::quiet_NaN();
      *iter_intensity = std::numeric_limits<float>::quiet_NaN();
      continue;
    }
    
    float range = dst[i].distance / 1000.f;  // distance unit transform to meters
    float dir_angle = ANGLE_TO_RADIAN(dst[i].angle);
    
    *iter_x = range * cos(dir_angle);
    *iter_y = range * sin(dir_angle);
    *iter_z = 0.0f;
    *iter_intensity = static_cast<float>(dst[i].intensity);
  }

  lidarpub->publish(output);
  end_scan_time = start_scan_time;
}

/********************* (C) COPYRIGHT SHENZHEN LDROBOT CO., LTD *******END OF
 * FILE ********/
