-- MySQL dump 10.13  Distrib 9.5.0, for macos15 (x86_64)
--
-- Host: localhost    Database: MetaAdsdb
-- ------------------------------------------------------
-- Server version	9.5.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ad_accounts`
--

DROP TABLE IF EXISTS `ad_accounts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ad_accounts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ad_account_id` bigint unsigned NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `currency` varchar(10) DEFAULT NULL,
  `account_creation_date` datetime DEFAULT NULL,
  `timezone` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `first_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `portfolio_id` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ad_account_id` (`ad_account_id`),
  KEY `idx_ad_accounts_portfolio` (`portfolio_id`),
  CONSTRAINT `fk_ad_accounts_portfolio` FOREIGN KEY (`portfolio_id`) REFERENCES `portfolios` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1816 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ad_daily_insights`
--

DROP TABLE IF EXISTS `ad_daily_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ad_daily_insights` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `impressions` bigint DEFAULT NULL,
  `reach` bigint DEFAULT NULL,
  `spend` decimal(18,2) DEFAULT NULL,
  `results` int DEFAULT NULL,
  `cost_per_result` decimal(18,2) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `ad_id` bigint unsigned DEFAULT NULL,
  `frequency` decimal(10,4) DEFAULT NULL,
  `checked_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ad_date` (`ad_id`,`date`),
  CONSTRAINT `fk_ad_daily_ad` FOREIGN KEY (`ad_id`) REFERENCES `ads` (`ad_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ad_posts`
--

DROP TABLE IF EXISTS `ad_posts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ad_posts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ad_id` bigint unsigned NOT NULL,
  `post_row_id` int NOT NULL,
  `link_type` enum('facebook_story','instagram_permalink') DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ad_posts_ad` (`ad_id`),
  KEY `idx_ad_posts_post` (`post_row_id`),
  CONSTRAINT `fk_ad_posts_ad` FOREIGN KEY (`ad_id`) REFERENCES `ads` (`ad_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ad_posts_post` FOREIGN KEY (`post_row_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ads`
--

DROP TABLE IF EXISTS `ads`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ads` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ad_id` bigint unsigned NOT NULL,
  `creative_id` bigint unsigned DEFAULT NULL,
  `adset_id` bigint unsigned NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `effective_status` varchar(50) DEFAULT NULL,
  `thumbnail_url` text,
  `image_url` text,
  `post_link` text,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `post_id` varchar(100) DEFAULT NULL,
  `campaign_id` bigint unsigned DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `start_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ad_id_UNIQUE` (`ad_id`),
  KEY `fk_ads_adset` (`adset_id`),
  KEY `idx_ads_creative_id` (`creative_id`),
  CONSTRAINT `fk_ads_adset` FOREIGN KEY (`adset_id`) REFERENCES `adsets` (`adset_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ads_creative` FOREIGN KEY (`creative_id`) REFERENCES `creative_ads` (`creative_id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_daily_insights`
--

DROP TABLE IF EXISTS `adset_daily_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `adset_daily_insights` (
  `id` int NOT NULL AUTO_INCREMENT,
  `adset_id` bigint unsigned NOT NULL,
  `results` int DEFAULT '0',
  `cost_per_result` decimal(18,2) DEFAULT NULL,
  `spend` decimal(18,2) DEFAULT NULL,
  `impressions` bigint DEFAULT NULL,
  `reach` bigint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `date` date NOT NULL,
  `frequency` decimal(10,4) DEFAULT NULL,
  `checked_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_adset_date` (`adset_id`,`date`),
  CONSTRAINT `fk_adi_adset` FOREIGN KEY (`adset_id`) REFERENCES `adsets` (`adset_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adsets`
--

DROP TABLE IF EXISTS `adsets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `adsets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `adset_id` bigint unsigned NOT NULL,
  `campaign_id` bigint unsigned NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `daily_budget` decimal(18,2) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `billing_event` varchar(50) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `optimization_goal` varchar(50) DEFAULT NULL,
  `ad_account_id` bigint unsigned DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `effective_status` varchar(50) DEFAULT NULL,
  `first_seen_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `real_status` enum('ACTIVE','PAUSED') DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `adset_id` (`adset_id`),
  KEY `fk_adsets_campaign` (`campaign_id`),
  KEY `fk_adsets_ad_account` (`ad_account_id`),
  CONSTRAINT `fk_adsets_ad_account` FOREIGN KEY (`ad_account_id`) REFERENCES `ad_accounts` (`ad_account_id`) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_adsets_campaign` FOREIGN KEY (`campaign_id`) REFERENCES `campaigns` (`campaign_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `billing`
--

DROP TABLE IF EXISTS `billing`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `billing` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ad_account_id` bigint unsigned NOT NULL,
  `currency` varchar(10) DEFAULT NULL,
  `last_activity_date` date DEFAULT NULL,
  `amount_spent` decimal(18,2) DEFAULT NULL,
  `balance` decimal(18,2) DEFAULT NULL COMMENT 'Could be removed',
  `spend_cap` decimal(18,2) DEFAULT NULL,
  `daily_spend_limit` decimal(18,2) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `account_status` int DEFAULT NULL,
  `disable_reason` int DEFAULT NULL,
  `checked_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_billing_ad_account` (`ad_account_id`),
  CONSTRAINT `fk_billing_ad_account` FOREIGN KEY (`ad_account_id`) REFERENCES `ad_accounts` (`ad_account_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=922 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaigns`
--

DROP TABLE IF EXISTS `campaigns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `campaigns` (
  `id` int NOT NULL AUTO_INCREMENT,
  `campaign_id` bigint unsigned NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `objective` varchar(255) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ad_account_id` bigint unsigned NOT NULL,
  `status` varchar(50) DEFAULT NULL,
  `effective_status` varchar(50) DEFAULT NULL,
  `first_seen_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `real_status` enum('ACTIVE','PAUSED') DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `campaign_id_UNIQUE` (`campaign_id`),
  KEY `fk_campaigns_ad_account` (`ad_account_id`),
  CONSTRAINT `fk_campaigns_ad_account` FOREIGN KEY (`ad_account_id`) REFERENCES `ad_accounts` (`ad_account_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaigns_daily_insights`
--

DROP TABLE IF EXISTS `campaigns_daily_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `campaigns_daily_insights` (
  `id` int NOT NULL AUTO_INCREMENT,
  `campaign_id` bigint unsigned NOT NULL,
  `date` date NOT NULL,
  `results` int DEFAULT '0',
  `cost_per_result` decimal(18,2) DEFAULT NULL,
  `spend` decimal(18,2) DEFAULT NULL,
  `impressions` bigint DEFAULT NULL,
  `reach` bigint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `frequency` decimal(10,4) DEFAULT NULL,
  `checked_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_campaign_date` (`campaign_id`,`date`),
  KEY `fk_campaign_id_idx` (`campaign_id`),
  CONSTRAINT `fk_cdi_campaign` FOREIGN KEY (`campaign_id`) REFERENCES `campaigns` (`campaign_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `creative_ads`
--

DROP TABLE IF EXISTS `creative_ads`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `creative_ads` (
  `id` int NOT NULL AUTO_INCREMENT,
  `creative_id` bigint unsigned NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `body` text,
  `effective_object_story_id` varchar(100) DEFAULT NULL,
  `instagram_permalink_url` text,
  `link_url` text,
  `page_id` bigint unsigned DEFAULT NULL,
  `thumbnail_url` text,
  `video_id` bigint unsigned DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `creative_sourcing_spec` json DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `first_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `creative_id_UNIQUE` (`creative_id`),
  KEY `fk_pages_idx` (`page_id`)
) ENGINE=InnoDB AUTO_INCREMENT=35511 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `page_ad_account`
--

DROP TABLE IF EXISTS `page_ad_account`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `page_ad_account` (
  `id` int NOT NULL AUTO_INCREMENT,
  `page_id` bigint unsigned NOT NULL,
  `ad_account_id` bigint unsigned NOT NULL,
  `first_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_page_adaccount` (`page_id`,`ad_account_id`),
  KEY `fk_pages_idx` (`page_id`),
  KEY `fk_paa_ad_account` (`ad_account_id`),
  CONSTRAINT `fk_paa_ad_account` FOREIGN KEY (`ad_account_id`) REFERENCES `ad_accounts` (`ad_account_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_paa_page` FOREIGN KEY (`page_id`) REFERENCES `pages` (`page_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pages`
--

DROP TABLE IF EXISTS `pages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `pages` (
  `id` int NOT NULL AUTO_INCREMENT,
  `page_id` bigint unsigned NOT NULL,
  `page_name` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `page_access_token` longtext,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_time` datetime DEFAULT NULL,
  `first_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ig_user_id` bigint unsigned DEFAULT NULL,
  `ig_username` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `page_id_UNIQUE` (`page_id`),
  KEY `idx_pages_ig_user_id` (`ig_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=215 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `portfolios`
--

DROP TABLE IF EXISTS `portfolios`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `portfolios` (
  `id` int NOT NULL AUTO_INCREMENT,
  `code` varchar(50) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` text,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `posts`
--

DROP TABLE IF EXISTS `posts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `posts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `page_id` bigint unsigned NOT NULL,
  `post_id` varchar(100) NOT NULL,
  `media_type` enum('IMAGE','VIDEO','REEL','STORY') DEFAULT NULL,
  `instagram_permalink_url` text,
  `thumbnail_url` text,
  `created_time` datetime DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `platform` enum('facebook','instagram') NOT NULL,
  `effective_object_story_id` varchar(100) DEFAULT NULL,
  `ig_media_id` varchar(50) DEFAULT NULL,
  `first_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `permalink_url` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_pages_page_post` (`page_id`,`post_id`),
  UNIQUE KEY `uq_posts_platform_post` (`platform`,`post_id`),
  KEY `page_id_idx` (`page_id`),
  KEY `idx_posts_page_time` (`page_id`,`created_time`),
  KEY `idx_posts_platform` (`platform`),
  KEY `idx_posts_platform_time` (`platform`,`created_time`),
  KEY `idx_posts_ig_media_id` (`ig_media_id`),
  KEY `idx_posts_effective_story` (`effective_object_story_id`),
  CONSTRAINT `page_id` FOREIGN KEY (`page_id`) REFERENCES `pages` (`page_id`)
) ENGINE=InnoDB AUTO_INCREMENT=191 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sync_checkpoints`
--

DROP TABLE IF EXISTS `sync_checkpoints`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sync_checkpoints` (
  `id` int NOT NULL AUTO_INCREMENT,
  `entity` varchar(50) NOT NULL,
  `scope_key` varchar(100) NOT NULL,
  `last_success_at` datetime DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_entity_scope` (`entity`,`scope_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sync_state`
--

DROP TABLE IF EXISTS `sync_state`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sync_state` (
  `id` int NOT NULL AUTO_INCREMENT,
  `sync_key` varchar(190) NOT NULL,
  `last_synced_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_sync_key` (`sync_key`)
) ENGINE=InnoDB AUTO_INCREMENT=83 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-03  0:25:17
