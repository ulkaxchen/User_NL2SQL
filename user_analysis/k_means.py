import numpy as np
class KMeans:
    def __init__(self, num_clusters = 4, max_iters=100):
        self.num_clusters = num_clusters
        self.max_iters = max_iters

    def fit(self, X):
        num_samples, num_features = X.shape
        self.centers = X[np.random.choice(num_samples, self.num_clusters, replace=False)]
        print(self.centers)
        for _ in range(self.max_iters):
            # distances = np.linalg.norm(X[:, np.newaxis])
            distances = np.sqrt((X - self.centers[:, np.newaxis]) ** 2).sum(axis=2)
            # (1, 1000, 2) - (4, 1, 2) -> (4, 1000, 2) -> (4, 1000)
            closest_centers = np.argmin(distances, axis=0)
            # print(closest_centers)
            new_centers = np.array([X[closest_centers == i].mean(axis=0) for i in range(self.num_clusters)])
            if np.all(self.centers == new_centers):
                break
            self.centers = new_centers
        
        return self
    
    def predict(self, X):
        distances = np.sqrt((X - self.centers[:, np.newaxis]) ** 2).sum(axis=2)
        closest_centers = np.argmin(distances, axis=0)
        return closest_centers  
    
km = KMeans(num_clusters=3)
X = np.array([[1, 2], [1, 4], [1, 0], [4, 2], [4, 4], [4, 0]])
km.fit(X)
print("Cluster centers:\n", km.centers)
predictions = km.predict(X)
print("Predicted cluster assignments:\n", predictions)