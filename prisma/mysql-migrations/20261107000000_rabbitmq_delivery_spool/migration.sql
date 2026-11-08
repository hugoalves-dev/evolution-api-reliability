CREATE TABLE `RabbitmqDelivery` (
  `id` VARCHAR(36) NOT NULL PRIMARY KEY,
  `instance` VARCHAR(255) NOT NULL,
  `eventId` VARCHAR(255) NOT NULL,
  `exchange` VARCHAR(255) NOT NULL,
  `queue` VARCHAR(255) NOT NULL,
  `route` VARCHAR(255) NOT NULL,
  `payload` LONGTEXT NOT NULL,
  `status` VARCHAR(16) NOT NULL,
  `attempts` INTEGER NOT NULL DEFAULT 0,
  `availableAt` DATETIME(3) NOT NULL,
  `leaseUntil` DATETIME(3),
  `leaseToken` VARCHAR(36),
  `createdAt` DATETIME(3) NOT NULL,
  `expiresAt` DATETIME(3) NOT NULL,
  INDEX `RabbitmqDelivery_due_idx` (`status`, `availableAt`, `leaseUntil`),
  INDEX `RabbitmqDelivery_expiry_idx` (`expiresAt`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
