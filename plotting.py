import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np



class Plotting:
	def createPositionPlot(self, xLabel, dataFile, fileName):
		chunkSize = 1000
		lowerThreshold = 10 * chunkSize
		higherThreshold = 200 * chunkSize

		position_vector, count_vector = np.loadtxt(dataFile, delimiter = ', ', \
										usecols = (0,1), dtype = int, unpack = True)
		
		lowerFlags = count_vector > lowerThreshold
		higherFlags = count_vector < higherThreshold

		thresholdFlags = np.logical_and(lowerFlags, higherFlags)
		filtered_position_vector = position_vector[thresholdFlags]
		filtered_count_vector = count_vector[thresholdFlags]
		
		print("Filtered out %.2f%%" % ((len(filtered_count_vector) / len(count_vector) * 100)))

		plt.plot(filtered_position_vector, filtered_count_vector, 'b.')
		plt.savefig(fileName)
	

 

	def createHeatMap2D(self, xLabel, yLabel, data, fileName):
		fig, ax = plt.subplots()
		heatmap = ax.pcolor(data, cmap=plt.cm.Reds)

		ax.set_xticks(np.arange(data.shape[0])+0.5, minor=False)
		ax.set_yticks(np.arange(data.shape[1])+0.5, minor=False)

		ax.invert_yaxis()
		ax.set_xticklabels(xLabel, minor=False)
		ax.set_yticklabels(yLabel, minor=False)

		plt.plot()
		plt.savefig(fileName)


'''		
		x = np.empty([max_y, max_x]) 
		
		x[:,:] = count_vector
		plt.tick_params(
		    axis='y',    
		    which='both',
		    left='off',
		    right='off',
		    labelright='off',
		    labelleft='off'
	    ) 

		plt.contourf(x)
		plt.savefig(fileName)
'''
