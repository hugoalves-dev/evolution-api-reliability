CREATE TABLE "RabbitmqDelivery" (
  "id" VARCHAR(36) NOT NULL PRIMARY KEY,
  "instance" VARCHAR(255) NOT NULL,
  "eventId" VARCHAR(255) NOT NULL,
  "exchange" VARCHAR(255) NOT NULL,
  "queue" VARCHAR(255) NOT NULL,
  "route" VARCHAR(255) NOT NULL,
  "payload" TEXT NOT NULL,
  "status" VARCHAR(16) NOT NULL,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "availableAt" TIMESTAMP(3) NOT NULL,
  "leaseUntil" TIMESTAMP(3),
  "leaseToken" VARCHAR(36),
  "createdAt" TIMESTAMP(3) NOT NULL,
  "expiresAt" TIMESTAMP(3) NOT NULL
);
CREATE INDEX "RabbitmqDelivery_due_idx" ON "RabbitmqDelivery" ("status", "availableAt", "leaseUntil");
CREATE INDEX "RabbitmqDelivery_expiry_idx" ON "RabbitmqDelivery" ("expiresAt");
